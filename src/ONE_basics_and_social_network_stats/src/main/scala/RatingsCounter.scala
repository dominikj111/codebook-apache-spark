package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

/** Count up how many of each star rating exists in the MovieLens 100K data set. */
object RatingsCounter {

  /** Our main function where the action happens */
  def mainx(args: Array[String]) {

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "RatingsCounter")

    // Load up each line of the ratings data into an RDD
    val lines = sc.textFile("../../ml-100k/u.data")

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
    val ratings = lines.map(x => x.toString().split("\t")(2))

    // TEST1
    println(ratings)

    // TEST2 countByValue example
    val rdd1 = sc.parallelize(Seq(("HR",5),("RD",4),("ADMIN",5),("SALES",4),("SER",6),("MAN",8)))
    var result = rdd1.countByValue

    // TEST3
    println(result)

    // Count up how many times each value (rating) occurs
    val results = ratings.countByValue

    // TEST4
    // ratings.getClass.getMethods.map(_.getName)
    //        .sorted
    //        .filter(_ matches "(?i).*index.*")
    //        .map(println _)

    // Sort the resulting map of (rating, count) tuples
    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
    sortedResults.foreach(println)

    sc.stop()
  }
}
