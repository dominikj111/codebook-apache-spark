package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import grizzled.slf4j._

object AverageCounting extends Logging {

    val logout = Logger("com.sundogsoftware.spark.AverageCounting")

    def mainx(args: Array[String]) {

        val sc = new SparkContext("local[*]", "AverageCounting")
        val lines = sc.textFile("../../SparkScala/fakefriends.csv")

        logout.info("Readed lines from the csv file, result: " + lines.getClass)

        val averageNames = lines.map(l => (l.split(",")(1),1)).reduceByKey((x,y) => x + y)

        printrdd(averageNames)

        sc.stop()
    }

    def breakRecordLine(line: String) { line.split(",")(1) }
    def printrdd(rdd: RDD[_]) { rdd.collect.foreach(println) }
}
