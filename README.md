# BDM_assignment

## Overview

You are required to analyze a dataset that represent library checkout records and library inventory.
The analysis includes writing the code for three queries.

## Dataset
You will work with a dataset that is in csv format and that represents details about Seattle Public
library Inventory and the checkout records from 2005 till 2017. The dataset is available from
Kaggle and can be downloaded from AWS S3 bucket. It contains the following three files:

### Checkouts By Title Data Lens 20XX: 
represents the records of all checkouts during the year
20XX.We are mainly interested in the columns: BibNumber and ItemType.
### Integrated Library System ILS Data Dictionary: 
it includes the codes of ItemTypes and their
details. We are mainly interested in the columns: Code (which could represent a code for
item type item location, ...), Code Type, Format Group, and Format Subgroup.
### Library Collection Inventory: 
includes information about all the library inventory. We are
mainly interested in the columns: BibNum and ItemLocation.

## Required Implementation
You are required to write the following functions:
### libraryItemsPerAuthor: 
find the total number of items in the library inventory per author.

### numberCheckoutRecordsP erFormat: 
given library checkout records and the dictionary of
library codes, find the total number of checkout occurrences for each item type specified by
a Format (Format Gorup + Format SubGroup).

### topKCheckoutLocations: 
given library checkout records and library inventory details, find
the top k locations that have the highest numbers of checkout records. Decode the location
using the library dictionary.

