# DataFrames and SparkSQL

You may not need to use RDDs very often in Spark, as most of the developments are currently happening with DataFrames and DataSets. DataSets are statically typed constructs used in Scala and Java. DataSets aren't available in Python.

To compile, run `sbt assembly`. To run, run `spark-submit --files winemag-data-130k-v2.csv.gz target/scala-2.12/SparkSqlWine-assembly-2.0.jar`

## Acknowledgements

This data was taken from this [Kaggle dataset](https://www.kaggle.com/zynicide/wine-reviews#winemag-data-130k-v2.csv)
