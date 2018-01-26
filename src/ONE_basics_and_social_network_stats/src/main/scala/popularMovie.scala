package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import grizzled.slf4j._

import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction

object popularMovie extends Logging {

    val logout = Logger("com.sundogsoftware.spark.popularMovie")

    def loadMovieNames(): Map[Int, String] = {

        // handle character encoding issue
        implicit val codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

        var movieNames: Map[Int, String] = Map()
        var lines = Source.fromFile("../../ml-100k/u.item").getLines()

        for(l <- lines){
            var fields = l.split('|')
            if(fields.length > 1) {
                movieNames += (fields(0).toInt -> fields(1))
            }
        }

        return movieNames
    }

    def mainx(args: Array[String]) {

        val sc = new SparkContext("local[*]", "popularMovie")
        val movieFile = sc.textFile("../../ml-100k/u.data")

        val nameDict = sc.broadcast(loadMovieNames)

        logout.info("Readed lines from the csv file, result: " + movieFile.getClass)

        val popular = movieFile.map(r => (r.split("\t")(1).toInt, 1)).reduceByKey((x,y) => x + y)

        val popularSorted = popular.map(r => (r._2, r._1)).sortByKey()

        prddn(rdd = popularSorted.map(r => (nameDict.value(r._2), r._1)), 100)

        sc.stop()
    }

    def prdd(rdd: RDD[_]) { rdd.collect.foreach(println) }
    def prddn(rdd: RDD[_], limit: Int) { rdd.collect.slice(0,limit).foreach(println) }
}
