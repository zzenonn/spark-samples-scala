package edu.ateneo.nrg.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.twitter._
import org.apache.log4j._
import org.apache.spark.SparkFiles._ 
import Utils._


/** Compute the number of words in a stream of text. */
object PrintTweets {

  /** Our main function where the action happens */
  def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    configTwitterCredentials(args(0))

    // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "PrintTweets")
  
    val ssc = new StreamingContext(sc, Seconds(2))

    ssc.checkpoint("file:///tmp/spark")

    val tweets = TwitterUtils.createStream(ssc, None)

    val statuses = tweets.map(status => status.getText())

    statuses.print

    ssc.start()
    ssc.awaitTermination()
    // results.foreach(println)
  }
    
}
  
