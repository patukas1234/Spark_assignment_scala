package dk.itu.BIDMT.F19.Exam.PartII

import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object LibraryCheckoutBatchProcessing {

  //create a spark session
  val spark = SparkSession
    .builder()
    .appName("LibraryCheckoutBatchProcessing")
    .master("local[4]")
    .getOrCreate()

  import spark.implicits._


  /**
    * Code to load the data from a csv file given the file path of that file
    *
    *
    * @param path
    * @return DataFrame of the loaded data
    */
  def dataLoader(path: String): DataFrame = {
    spark.read
      .option("header","true")
      .option("inferschema","true")
      .csv(path)
  }

  /**
    * 
    * qurey the library inventory to find the number of items in the inventory written/created by each author
    *
    * @param libraryInventoryDF
    * @return A DaraFrame of two columns: Author, NumPublications
    */
  def libraryItemsPerAuthor(libraryInventoryDF: DataFrame): DataFrame = {
    libraryInventoryDF   // .groupBy("Author").count()
      .filter($"Author".isNotNull)
      .groupBy(col1 = "Author")
      .count()
  }
  /**
    * 
    * query the checkout records and the library dictionary to find
    * the number of checked out items per Fromat Group - Format Subgroup pair
    *
    * Note that multiple ItemType code could have the same Format Group: Format Subgroup combination
    * drmfmnp,	Microfilm: Dummy Newspaper,	ItemType,	Media,	Film, ,
    * drmfper,	Microfilm: Dummy Periodical,	ItemType,	Media,	Film, ,
    *
    *
    * @param checkoutDF
    * @param dataDictionaryDF
    * @return A DataFrame of two columns: Format,CheckoutCount
    */
  def numberCheckoutRecordsPerFormat(checkoutDF: DataFrame, dataDictionaryDF: DataFrame): DataFrame = {
    //checkoutDF.persist()
    //dataDictionaryDF.persist()
    val conCats = udf( (first: String, second: String) => { first + ":" + second} )


    val df2 = dataDictionaryDF.withColumn("new_col", when(col("Format Subgroup").isNull, "")
      .otherwise(col("Format Subgroup")))
      .withColumn("Format", conCats($"Format Group", $"new_col"))



    checkoutDF
      .join(df2.filter($"Code Type".like("ItemType")) , checkoutDF("ItemType") === df2("Code"), "inner")
      //.join(dataDictionaryDF.filter("Code Type" == "ItemLocation"),dataDictionaryDF("CodeType") === libraryInventoryDF("ItemLocation"), "inner")
      .groupBy("Format").count()

    //checkoutDF.unpersist()
    //dataDictionaryDF.unpersist()

  }

  /**
    * 
    * query the checkout records and the library inventory details to
    * find the top k library locations where the most checkouts happened
    *
    * The values stored in the ItemLocation column of the library inventory file is a code.
    * Therefore, you will need to decode the location from the description found in the library dictionary
    * Note: for codes that represent library locations, the value in Code Type column is "ItemLocation"
    * and the details of the location are in the Description column
    *
    * @param checkoutDF
    * @param libraryInventoryDF
    * @param dataDictionaryDF
    * @param k
    * @return A DataFrame of two columns: ItemLocationDescription, NumCheckoutItemsAtLocation - num of records in this dataframe is equal to k
    */
  def topKCheckoutLocations(checkoutDF: DataFrame, libraryInventoryDF: DataFrame, dataDictionaryDF: DataFrame, k: Int): DataFrame ={
    //checkoutDF.persist()
    //libraryInventoryDF.persist()
    //dataDictionaryDF.persist()

    val dicti = dataDictionaryDF.filter($"Code Type".like("ItemLocation"))

    val lib_decoded = libraryInventoryDF.join(dicti,dataDictionaryDF("Code") === libraryInventoryDF("ItemLocation"), "inner")

    checkoutDF
      // .withColumnRenamed("BibNumber","BibNum")
      .join(lib_decoded, checkoutDF("BibNumber") === lib_decoded("BibNum"), "inner")
      .groupBy("Description").count()
      // .count()
      .orderBy(desc("count"))
      .limit(k)

   // checkoutDF.unpersist()
   // libraryInventoryDF.unpersist()
   // dataDictionaryDF.unpersist()

  }

  def main(args: Array[String]): Unit = {
    //load configuration
    val config = ConfigFactory.load()

    //check that the paths for input files exist in the config, otherwise exit
    if (!config.hasPath("BIDMT.Exam.Batch.checkoutData") ||
      !config.hasPath("BIDMT.Exam.Batch.dataDictionary") ||
      !config.hasPath("BIDMT.Exam.Batch.libraryInventory")) {
      println("Error, configuration file does not contain the file paths for input datasets!")
      spark.close()
    }

    //read input file paths from the configuration file
    val checkoutFilePath = config.getString("BIDMT.Exam.Batch.checkoutData")
    val dataDictionaryFilePath = config.getString("BIDMT.Exam.Batch.dataDictionary")
    val libraryInventoryFilePath = config.getString("BIDMT.Exam.Batch.libraryInventory")

    //read output files path from the configuration file
    val outFilesPath = config.getString("BIDMT.Exam.Batch.outPath")

   /* val checkoutDF = dataLoader(checkoutFilePath)
    val dataDictionaryDF = dataLoader(dataDictionaryFilePath)
    val libraryInventoryDF = dataLoader(libraryInventoryFilePath)
   // libraryItemsPerAuthor(libraryInventoryDF)
     //   .show(5)
      //.write
      // .mode("overwrite")
      //.csv(outFilesPath + "/q1")

    //topKCheckoutLocations(checkoutDF, libraryInventoryDF, dataDictionaryDF, 7)
    numberCheckoutRecordsPerFormat(checkoutDF, dataDictionaryDF)
      .show(5) */

    if (args.length == 0) {
      //call all queries
      //load data
      val checkoutDF = dataLoader(checkoutFilePath)
      val dataDictionaryDF = dataLoader(dataDictionaryFilePath)
      val libraryInventoryDF = dataLoader(libraryInventoryFilePath)

      //read value of k from configuration file
      val numLibraryLocations =
        if (config.hasPath("BIDMT.Exam.Batch.numLibraryLocations"))
          config.getInt("BIDMT.Exam.Batch.numLibraryLocations")
        else 30 //we are setting the default value to 30

      //Query 1
      libraryItemsPerAuthor(libraryInventoryDF)
        .write
        .mode("overwrite")
        .csv(outFilesPath + "/q1")
      //Query 2
      numberCheckoutRecordsPerFormat(checkoutDF, dataDictionaryDF)
        .write
        .mode("overwrite")
        .csv(outFilesPath + "/q2")
      //Query 3
      topKCheckoutLocations(checkoutDF, libraryInventoryDF, dataDictionaryDF, numLibraryLocations)
        .write
        .mode("overwrite")
        .csv(outFilesPath + "/q3")

    } else {
      args(0).toInt match {
        case 1 => {
          //Query 1
          //load data
          val libraryInventoryDF = dataLoader(libraryInventoryFilePath)
          libraryItemsPerAuthor(libraryInventoryDF)
            .write
            .mode("overwrite")
            .csv(outFilesPath + "/q1")
        }
        case 2 => {
          //Query 2
          //load data
          val checkoutDF = dataLoader(checkoutFilePath)
          val dataDictionaryDF = dataLoader(dataDictionaryFilePath)
          numberCheckoutRecordsPerFormat(checkoutDF, dataDictionaryDF)
            .write
            .mode("overwrite")
            .csv(outFilesPath + "/q2")
        }
        case 3 => {
          //Query 3
          //load data
          val checkoutDF = dataLoader(checkoutFilePath)
          val dataDictionaryDF = dataLoader(dataDictionaryFilePath)
          val libraryInventoryDF = dataLoader(libraryInventoryFilePath)
          //read value of k from configuration file
          val numLibraryLocations =
            if (config.hasPath("BIDMT.Exam.Batch.numLibraryLocations"))
              config.getInt("BIDMT.Exam.Batch.numLibraryLocations")
            else 30 //we are setting the default value to 30

          topKCheckoutLocations(checkoutDF, libraryInventoryDF, dataDictionaryDF, numLibraryLocations)
            .write
            .mode("overwrite")
            .csv(outFilesPath + "/q3")
        }
        case _ => println("Usage for LibraryCheckoutBatchProcessing: Optional selected query can be 1, 2, or 3")
      }
    }

    //stop spark
    spark.close()
  }
}
