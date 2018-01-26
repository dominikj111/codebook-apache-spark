package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import grizzled.slf4j._
import scala.math.min
import scala.math.max

object Filters extends Logging {

    val logout = Logger("com.sundogsoftware.spark.Filters")

    def mainx(args: Array[String]) {

        val sc = new SparkContext("local[*]", "Filters")
        val lines = sc.textFile("../../SparkScala/1800.csv")

        logout.info("Readed lines from the csv file, result: " + lines.getClass)

        val rddInputs = lines.map(l => l.split(",")).map(l => (l(0),l(2),l(3).toFloat * 1.0f * (9.0f / 5.0f) + 32f)) // convert degrees to faranheits

        val minTemps = rddInputs.filter(r => r._2 == "TMIN")
        val prcpTemp = rddInputs.filter(r => r._2 == "PRCP")

        val minTempStations = minTemps.map(r => (r._1,r._3.toFloat)).reduceByKey((x,y) => min(x,y)) // .toFloat is redundant
        val prcpTempStations= prcpTemp.map(r => (r._1,r._3)).reduceByKey((x,y) => max(x,y))

        printrdd(prcpTempStations)

        printrdd(minTempStations)

        val finalOutput = minTempStations.collect()

        for(result <- finalOutput){
            val stationid = result._1
            val temp = result._2
            val formatedTemp = f"$temp%.2f F"
            println(s"$stationid minimum temperature: $formatedTemp")
        }

        sc.stop()
    }

    def printrdd(rdd: RDD[_]) { rdd.collect.foreach(println) }
}
