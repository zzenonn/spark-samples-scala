package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 


/** Compute the frequency of words in a body of text. */
object WordFrequency {
  
  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
        
    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "WordFrequency")
  
    // Load each line of the source data into an RDD
    val lines = sc.textFile(SparkFiles.get("book.txt"))
    
    val words = lines.flatMap(x => x.trim.toLowerCase().split("\\W+"))

    val wordsFiltered = words.filter(x => x != "")

    val totalWords = wordsFiltered.count()

    val wordMap = wordsFiltered.map(x => (x, 1.0))

    val wordCount = wordMap.reduceByKey( (x,y) => x + y)

    val wordCountFreq = wordCount.mapValues(x => (x.toDouble/totalWords.toDouble))
    
    val results = wordCountFreq.takeOrdered(20)(Ordering[Double].reverse.on(x => x._2))

    results.foreach(println)
  }
    
}
  
