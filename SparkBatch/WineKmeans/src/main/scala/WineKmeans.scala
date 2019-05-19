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



/** Run the Kmeans algorithm on text data. */
object WineKmeans {
  

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    val log = Logger.getLogger("edu.ateneo.nrg.spark.WineKmeans")
    log.setLevel(Level.INFO)
        
    // Create a SparkContext using every core of the local machine
    val spark = SparkSession
        .builder
        .appName("SparkSqlWine")
        .master("local[*]")
        .getOrCreate()

    val schema = StructType(Array(
        StructField("id", IntegerType),
        StructField("country", StringType),
        StructField("description", StringType),
        StructField("desgination", StringType),
        StructField("points", IntegerType),
        StructField("price", DoubleType),
        StructField("province", StringType),
        StructField("region_1", StringType),
        StructField("region_2", StringType),
        StructField("taster_twitter_handle", StringType),
        StructField("title", StringType),
        StructField("variety", StringType),
        StructField("winery", StringType)
    ))
  
    // Load each line of the source data into an RDD
    val wineReviews = spark.read
        .format("csv")
        .option("header", "true")
        .option("multiLine", "true")
        .option("sep", ",")
        .option("quote", "\"")
        .schema(schema)
        .load(SparkFiles.get("winemag-data-130k-v2.csv.gz"))
    
    log.info("Filtering out bad data")
    // Filter out bad data
    val filteredWineReviews = wineReviews.filter("id is not null and country is not null and description is not null")
    
    // Creates a User defined function that will count the number of elements in an
    // array in a given column.
    val countTokens = udf((words: Seq[String]) => words.size, IntegerType)
    
    // Converts string to lowercase, then splits by regex. Denotes matching pattern, not splitting gaps.
    val regexTokenizer = new RegexTokenizer()
        .setInputCol("description")
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
        
    val comprehensiveTokenizerModel = comprehensiveTokenizer.fit(filteredWineReviews)

    val bigramDataFrame = comprehensiveTokenizerModel.transform(filteredWineReviews)

    log.info("Combining unigrams and bigrams")

    val finalWords = bigramDataFrame.withColumn("tokens", concat(col("filteredWords"), col("bigrams")))

    finalWords.select("description", "tokens").show(22, false)
    
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
    
    clustered.select("tokens", "prediction", "variety", "points", "price").filter("prediction = 0").show(truncate=false)
    clustered.select("tokens", "prediction", "variety", "points", "price").filter("prediction = 1").show(truncate=false)
    clustered.select("tokens", "prediction", "variety", "points", "price").filter("prediction = 2").show(truncate=false)
    
    
    spark.stop()
  }
    
}
  
