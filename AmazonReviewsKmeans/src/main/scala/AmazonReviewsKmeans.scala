package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml._
import org.apache.spark.ml.feature._
import org.apache.spark.ml.clustering._
import org.apache.spark.ml.feature._



/** Compute the average number of friends by age in a social network. */
object AmazonReviewsKmeans {
  

  /** Our main function where the action happens */
  def main(args: Array[String]) {

    val input = args(0)
    val output = args(1)
   
    // Set the log level to only print errors
    val log = Logger.getLogger("edu.ateneo.nrg.spark.AmazonReviewsKmeans")
    log.setLevel(Level.INFO)

    log.info("Output Folder set to: " + output)
        
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
        .builder
        .appName("AmazonReviewsKmeans")
        .getOrCreate()
  
    // Load Parquet file
    val amazonReviews = spark.read
        .option("basePath", "s3://amazon-reviews-pds/parquet/")
        .parquet(input)
    
    log.info("Filtering out bad data")
    // Filter out bad data
    val filtererdAmazonReviews = amazonReviews.filter("review_body is not null")
    
    // Creates a User defined function that will count the number of elements in an
    // array in a given column.
    val countTokens = udf((words: Seq[String]) => words.size, IntegerType)
    
    // Converts string to lowercase, then splits by regex. Denotes matching pattern, not splitting gaps.
    val regexTokenizer = new RegexTokenizer()
        .setInputCol("review_body")
        .setOutputCol("words")
        .setPattern("\\w+").setGaps(false)
        
    // Remove stop words. You can also include your own list of stopwords.
    val remover = new StopWordsRemover()
        .setInputCol(regexTokenizer.getOutputCol)
        .setOutputCol("filteredWords")
    
    // Creates n-grams from tokens.
    val bigram = new NGram()
        .setN(2)
        .setInputCol(remover.getOutputCol)
        .setOutputCol("bigrams")
        
    // Creates a Spark ML Pipeline that specifies different models that will
    // apply at different stages.
    val comprehensiveTokenizer = new Pipeline()
        .setStages(Array(regexTokenizer, remover, bigram))
        
    val comprehensiveTokenizerModel = comprehensiveTokenizer.fit(filtererdAmazonReviews)

    val bigramDataFrame = comprehensiveTokenizerModel.transform(filtererdAmazonReviews)

    log.info("Combining unigrams and bigrams")

    val finalWords = bigramDataFrame.withColumn("tokens", concat(col("filteredWords"), col("bigrams")))

    finalWords.select("review_body", "tokens").show(22, false)
    
    // Get the raw count of each of the terms/tokens
    val cv = new CountVectorizer()  
        .setInputCol("tokens")
        .setOutputCol("rawFeatures") 
        .setMinDF(2.0) // minDF=2.0 means a token needs to appear at least twice for it to be considered part of the 
        
    // Get the logarithmically scaled relevance of each term based on the occurence of
    // the term in the entire document
    val idf = new IDF()
        .setInputCol(cv.getOutputCol)
        .setOutputCol("features")
        
    // By default, will add the predictions column to the current DF. 
    // Targets a column named "features" by default.
    val kmeans = new BisectingKMeans()
        .setK(3)
        .setSeed(4)
        
    log.info("Clustering data")
    
    val clusteringPipeline = new Pipeline()
        .setStages(Array(cv, idf, kmeans))
        
    val clusteringPipelineModel = clusteringPipeline.fit(finalWords)
    
    val clustered = clusteringPipelineModel.transform(finalWords)
    
    // Count the number of members per cluster.
    clustered.groupBy("prediction").count().orderBy("prediction").show()
    
    clustered.select("tokens", "prediction").filter("prediction = 0").withColumn("tokens", col("tokens").cast("string")).write.format("csv").save(output + "/prediction1")
    clustered.select("tokens", "prediction").filter("prediction = 1").withColumn("tokens", col("tokens").cast("string")).write.format("csv").save(output + "/prediction2")
    clustered.select("tokens", "prediction").filter("prediction = 2").withColumn("tokens", col("tokens").cast("string")).write.format("csv").save(output + "/prediction3")
    
    
    spark.stop()
  }
    
}
  
