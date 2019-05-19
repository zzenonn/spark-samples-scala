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
object TweetHashtagCount {

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

    // Get all hashtags
    val hashTags = tweets.flatMap(status => status.getText.split(" ").filter(_.startsWith("#")))

    val topCountPerMin = hashTags.map((_, 1)).reduceByKeyAndWindow((x,y) => x + y, windowDuration=Seconds(60), slideDuration=Seconds(10))
                     .transform(_.sortByKey(false))

    val topCountPer10Sec = hashTags.map((_, 1)).reduceByKeyAndWindow((x,y) => x + y, windowDuration=Seconds(10), slideDuration=Seconds(2))
                     .transform(_.sortByKey(false))

    topCountPerMin.foreachRDD(rdd => {
      val topList = rdd.takeOrdered(20)(Ordering[Double].reverse.on(x => x._2))
      println("\nPopular topics in last 60 seconds (%s total):".format(rdd.count()))
      topList.foreach(println)
    })

    topCountPer10Sec.foreachRDD(rdd => {
      val topList = rdd.takeOrdered(20)(Ordering[Double].reverse.on(x => x._2))
      println("\nPopular topics in last 10 seconds (%s total):".format(rdd.count()))
      topList.foreach(println)
})

    ssc.start()
    ssc.awaitTermination()
    // results.foreach(println)
  }
    
}
  
