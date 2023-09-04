# Example Feature Pipeline for both Spark and Pandas

This folder contains code to create features from World of Warcraft data.
The same example is created for both spark and pandas.

To get started first download the data:
`curl https://storage.googleapis.com/shareddatasets/wow.parquet -o data/wow.parquet`

## Spark example V1 vs V2

For the spark example we've created two different versions which both have their benefits and drawbacks.
See the notebooks for the details. The high level differences are as follows.

### Spark example V1
Keeps the code simpler (shorter) at the cost of losing column-level lineage.

### Spark example V2
Needs more lines of code (added complexity) but makes it easier to test and track (lineage) single transformations.
