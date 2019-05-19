package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 


/** Compute the average number of friends by age in a social network. */
object AveragePopulation {
  
  /** A function that splits a line of input into (region, population) tuples. */
  def parseLine(line: String) = {
      // Split by tabs
      val fields = line.split("\t")
      // Extract the age and numFriends fields, and convert to integers
      val region = fields(1)
      val population = fields(2).toDouble
      // Create a tuple that is our result.
      (region, population)
  } 
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "AveragePopulation")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile(SparkFiles.get("countries.csv"))
    
    // Use our parseLines function to convert to (region, population) tuples
    val rdd = lines.map(parseLine)
    
    // MapValues map the current value in the key-value pair to a new key value pair. They key remains the same.
    // In this case, the key is the region, and the new value is the tuple (<population>, 1).
    // The value then becomes another tuple. The reduce by key function takes the value of the first 
    // entry of the key(x), and the second entry of the key (y), then performs operations on it. In this case,
    // since x and y are tuples, we need to also specify the index we're looking at.
    val totalsPerRegion = rdd.mapValues((_, 1)).reduceByKey( (x._1 + y._1, x._2 + y._2))
    
    // After running the function in the previous line, the entire key-value pair would look something like
    // (key : <region>, value : (<total population>, <number of countries>)). Given this, we can now calculate
    // the average per region by dividing the two values.
    val averageByRegion = totalsPerRegion.mapValues(x => x._1 / x._2)
    
    // In spark, nothing happens until the application is run which is in this next step.
    // The function returns just a regular scala array object.
    val results = averageByRegion.collect()
    
    // Sort and print the final results.
    results.sorted.foreach(println)
  }
    
}
  
