package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 


/** Compute the average number of friends by age in a social network. */
object CountryCount {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "CountryCount")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile(SparkFiles.get("countries.csv"))
    
    // The input to the RDD map function is another function. In this case, it is a scala
    // function that splits the tab delimited dataset and only gets the second column of data.
    val regions = lines.map(x => x.split("\t")(1))
    
    val results = regions.countByValue()

    val sorted = results.toList.sortBy (-_._2)   

    sorted.foreach(println)
  }
    
}
  
