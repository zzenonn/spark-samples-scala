# Kmeans on Wine Reviews

This next application demonstrates the use of the Kmeans clustering algorithm using Spark along with some feature extraction on text data. After some minimal changes, this can even be run on a cluster.

To compile, run `sbt assembly`. To run, run `spark-submit --files winemag-data-130k-v2.csv.gz target/scala-2.11/WineKmeans-assembly-1.0.jar`

*Note: It is highly suggested that you use a virtual machine to run this application. As this requires very heavy processing, this may crash your computer.*

## Acknowledgements

This data was taken from this [Kaggle dataset](https://www.kaggle.com/zynicide/wine-reviews#winemag-data-130k-v2.csv)
