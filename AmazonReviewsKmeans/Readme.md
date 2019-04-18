# Kmeans on Wine Reviews

This next application demonstrates the use of the Kmeans clustering algorithm using Spark along with some feature extraction on text data. **This is meant to be run on EMR.** Specify the class as `edu.ateneo.nrg.spark.AmazonReviewsKmeans`. This application takes two arguments which is the input location and output location respectively in the form of an S3 bucket e.g. `s3://cs195.15/output/kmeans_amazon_reviews`. The input must be a subdirectory of `s3://amazon-reviews-pds/parquet/`.

## Acknowledgements

This data was taken from this [Amazon Open Reviews Dataset](https://registry.opendata.aws/amazon-reviews/)
