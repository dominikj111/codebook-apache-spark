package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import grizzled.slf4j._
import scala.math.min
import scala.math.max

object flatMapBook extends Logging {

    val logout = Logger("com.sundogsoftware.spark.flatMapBook")

    def mainx(args: Array[String]) {

        val sc = new SparkContext("local[*]", "flatMapBook")
        val lines = sc.textFile("../../SparkScala/bookdev.txt")

        logout.info("Readed lines from the csv file, result: " + lines.getClass)

        // val words = lines.flatMap(_.split(" ")).map(w => (w, 1))
        val words = lines.flatMap(_.split("\\W+")).map(w => (w, 1))

        // println(words.countByValue().getClass) // -> scala.collection.immutable.HashMap$HashTrieMap
        // words.countByValue().foreach(println) // -> ex. ((vision,1),1)

        // printrdd(words) // ok
        // printrdd(sc.parallelize(words.countByValue().toSeq)) // ok

        // val wordsCount1 = words.countByValue()
        // wordsCount1.foreach(println) // -> ((noise,1),3)

        val wordsCount = words
            .reduceByKey((x,y) => x + y)
            .map(r => (r._1.toLowerCase.length, (r._1.toLowerCase,r._2)))
            // .filter(
            //     r => !List(
            //         "is","a","an","if","it","by","s","re","be","to"
            //     ).contains(r._1)
            // )
            .sortByKey()
            .map(r => (r._2._1, r._2._2))
        // val wordsCount = words.reduceByKey((x,y) => x + y).map(r => (r._2,r._1.toLowerCase)).sortByKey()
        printrdd(wordsCount) // -> (referrer,3)

        sc.stop()
    }

    def printrdd(rdd: RDD[_]) { rdd.collect.foreach(println) }
}
