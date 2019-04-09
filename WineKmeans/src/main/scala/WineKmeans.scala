package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.desc


/** Compute the average number of friends by age in a social network. */
object WineKmeans {
  
  case class Person(ID:Int, name:String, age:Int, numFriends:Int)

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
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
    
    
    spark.stop()
  }
    
}
  
