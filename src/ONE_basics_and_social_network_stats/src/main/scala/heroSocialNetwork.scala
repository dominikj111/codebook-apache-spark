package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext._
import grizzled.slf4j._

import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction

import scala.math.max

// object myImplicits {
//     implicit class rddFliper(val rdd: RDD[Tuple2[_,_]]){
//         def flip(): RDD[Tuple2[_,_]] = rdd.map(x => (x._2, x._1))
//     }
// }

// class CustomFunctions(rdd: RDD[Tuple2[_,_]]) {
//     def flip(): RDD[Tuple2[_,_]] = rdd.map(x => (x._2, x._1))
// }

// object CustomFunctions {
//     implicit def addCustomFunctions(rdd: RDD[Tuple2[_,_]]) = new CustomFunctions(rdd)
// }

// implicit class stringFlipper(val s: String){ def flip(): String = s(1).toString + s(0).toString }

object RDDImplicits2{
    implicit class RichRDDT2[A,B](rdd: org.apache.spark.rdd.RDD[(A,B)]) {
        def flip = { rdd.map(element => (element._2, element._1)) }
    }
}

object heroSocialNetwork extends Logging {

    val logout = Logger("com.sundogsoftware.spark.heroSocialNetwork")

    def loadNameDict(): Map[Int, String] = {

        // handle character encoding issue
        implicit val codec = Codec("UTF-8")
        codec.onMalformedInput(CodingErrorAction.REPLACE)
        codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

        var names: Map[Int, String] = Map()
        var lines = Source.fromFile("../../SparkScala/Marvel-names.txt").getLines()

        for(l <- lines){
            var fields = l.split(" \"")
            if(fields.length > 1) {
                names += (fields(0).toInt -> fields(1).replace("\"","").trim())
            }
        }

        return names
    }

    def parseNames(line: String): Option[(Int, String)] = {
        var fields = line.split("\"")
        if(fields.length > 1) Some(fields(0).trim().toInt, fields(1)) else None
    }

    def countCoOccurences(line: String) = {
        var elements = line.split("\\s+")
        (elements(0).trim().toInt,elements.length - 1)
    }

    def mainx(args: Array[String]) {

        import RDDImplicits2.RichRDDT2

        // VERSION 1

        val sc = new SparkContext("local[*]", "heroSocialNetwork")
        // val marvelGraphFile = sc.textFile("../../SparkScala/Marvel-graph.txt")

        val nameDict = sc.broadcast(loadNameDict)

        // VERSION 2

        val marvelNames = sc.textFile("../../SparkScala/Marvel-names.txt").flatMap(parseNames)
        val marvelGraphPair = sc.textFile("../../SparkScala/Marvel-graph.txt").map(countCoOccurences).reduceByKey((a,b) => a + b)


        val mostPopular = marvelGraphPair.flip.max
        val mostPopularName = marvelNames.lookup(mostPopular._2)(0)

        println(s"$mostPopularName is most popular superhero with ${mostPopular._1} co-appearances")


        val marvelDownTen = marvelGraphPair.flip.sortByKey().take(10).map(r => (nameDict.value(r._2), r._1))
        val marvelTopTen = marvelGraphPair.flip.sortByKey(false).take(10).map(r => (nameDict.value(r._2), r._1))

        println("-- Down 10 --")
        marvelDownTen.foreach(println)
        println("-- Top 10 --")
        marvelTopTen.foreach(println)


        // val heroComicsOccurrences = marvelGraphFile.flatMap(_.split("\\D+")).map(x => (x.toInt,1)).reduceByKey((a,b) => a+b)
        // val heroComicsOccurrences = marvelGraphFile.map(x => (x.split("\\D+")(0).toInt, 1) ).reduceByKey((a,b) => a+b)

        // val heroBuddies = marvelGraphFile.map(_.split("\\D+")).map(x => (x.head.toInt, x.tail.length)).reduceByKey((a,b) => a+b)

        // prddn(heroComicsOccurrences.map(x => (nameDict.value(x._1),x._2)).map(r => (r._2, r._1)).sortByKey(false).map(r => (r._2, r._1)), 5)

        sc.stop()
    }

    def prdd(rdd: RDD[_]) { rdd.collect.foreach(println) }
    def prddn(rdd: RDD[_], limit: Int) { rdd.collect.slice(0,limit).foreach(println) }
    def flipRdd(rdd: RDD[Tuple2[_,_]]): RDD[Tuple2[_,_]] = {
        return rdd.map(r => (r._2, r._1))
    }
}
