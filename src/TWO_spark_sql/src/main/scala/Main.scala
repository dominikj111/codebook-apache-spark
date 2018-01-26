
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd._

import RDDImplicits.RichRDDT1, RDDImplicits.RichRDDT2

import grizzled.slf4j._

object Main extends App with Logging {

	val logout = Logger("com.sundogsoftware.spark.Main")

	val sc = new SparkContext("local[*]", "SecondSparkApp")

	val moviesFile = sc.textFile("../../ml-100k/u.data.less")

	val moviesRDD = moviesFile.map( _.split("\t") ).map( x => (x(0).toInt, (x(1).toInt, x(2).toInt)) ) // (userID,(movieID,rating))

	val joinedMoviesRDD = moviesRDD.join(moviesRDD) // (userID,((movieID1,rating1),(movieID2,rating2)))

	val filteredDuplicities = joinedMoviesRDD.filter((a,b) => a._2._1._1 != a._2._2._1)

	joinedMoviesRDD.printn(10)

	sc.stop()
}