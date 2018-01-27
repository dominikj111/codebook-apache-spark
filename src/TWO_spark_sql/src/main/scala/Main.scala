
import org.apache.spark._
import org.apache.spark.SparkContext._

import org.apache.spark.rdd._

import RDDImplicits._, FlowImplicits._, LocalSparkUtils._

import grizzled.slf4j._

import scala.math.sqrt

object Main extends App with Logging {

	val logout = Logger("com.sundogsoftware.spark.Main")

	val sc = new SparkContext("local[*]", "SecondSparkApp")

	// val moviesFile = sc.textFile("../../ml-100k/u.data")
	// used reduced version due to java.lang.OutOfMemoryError: GC overhead limit exceeded
	// another solution could be to use .partitionBy(new HashPartitioner(10)) - didn't work, need to tweak
	// 1. val moviesFile = sc.textFile("../../ml-100k/u.data.less")

	// 2. val moviesRDD = moviesFile.map( _.split("\t") ).map( x => (x(0).toInt, (x(1).toInt, x(2).toInt)) ) // (userID,(movieID,rating))

	// rdd1 = (id1, v1),(id1, v2),(id2, v1)
	// rdd2 = (id1, v3),(id1, v4),(id3, v2)
	// rdd1.join(rdd2) = (id1,(v1,v3)),(id1,(v1,v4)),(id1,(v2,v3)),(id1,(v2,v4))

	// 3. val joinedMoviesRDD = moviesRDD.join(moviesRDD) // (userID,((movieID1,rating1),(movieID2,rating2)))

	val movieNameDict = loadDict[Int, String](
		path = "../../ml-100k/u.item",
		dataDelimiter = "\\|",
		keyIndex = 0,
		valueIndex = 1,
		keyTransform = _.trim().toInt,
		valueTransform = _.trim()
	)

	type MovieRating = (Int, Int)
	type Ratings = (MovieRating, MovieRating)
	type RatingPairs = Iterable[MovieRating]

	val movieStatsRDD = sc.textFile("../../ml-100k/u.data.less")

		.map( _.split("\t") )

		.map( x => (x(0).toInt, (x(1).toInt, x(2).toInt)) ) // (userID,(movieID,rating))

		.processAndGetResult( x => x.join(x) ).asInstanceOf[RDD[(_,_)]] // => implicit rdd.join(rdd) => (userID,((movieID,rating),(movieID,rating)))

		.filter( x => {
			val ratings: Ratings = x._2.asInstanceOf[Ratings]
			val movieRat1: MovieRating = ratings._1
			val movieRat2: MovieRating = ratings._2

			movieRat1._1 < movieRat2._1
		}) // filtering out ( ... ,((3, ... ),(3, ... ))) + filter out ( ... ,((3, ... ),(5, ... ))) => ( ... ,((5, ... ),(3, ... ))) will left

		.map( x => {
			val ratings: Ratings = x._2.asInstanceOf[Ratings]
			val movieRat1: MovieRating = ratings._1
			val movieRat2: MovieRating = ratings._2

			((movieRat1._1, movieRat2._1),(movieRat1._2, movieRat2._2))
		}) // ((movieid1, movieid2),(rating1, rating2))

		.groupByKey() // (moviePairs) -> (collection of user ratings) => (movieid1, movieid2),((rating1, rating2),(rating3, rating4),(rating5, rating6),(rating7, rating8)))

		.mapValues( (ratingPairs: RatingPairs) => {

			var numPairs:Int = 0
		    var sum_xx:Double = 0.0
		    var sum_yy:Double = 0.0
		    var sum_xy:Double = 0.0

		    for (pair <- ratingPairs) {
		      val ratingX = pair._1
		      val ratingY = pair._2

		      sum_xx += ratingX * ratingX
		      sum_yy += ratingY * ratingY
		      sum_xy += ratingX * ratingY
		      numPairs += 1
		    }

		    val numerator:Double = sum_xy
		    val denominator = sqrt(sum_xx) * sqrt(sum_yy)

		    var score:Double = 0.0
		    if (denominator != 0) {
		      score = numerator / denominator
		    }

		    (score, numPairs)
		})

	val scoreThreshold = 0.95
  	val coOccurenceThreshold = 20.0
  	val movieID:Int = 181 // Return of the Jedi (1983)

  	val result = movieStatsRDD.filter( x => {
		val pair = x._1
		val sim = x._2

		(pair._1 == movieID || pair._2 == movieID) && sim._1 > scoreThreshold && sim._2 > coOccurenceThreshold
  	}).map(x => (x._2, x._1)).sortByKey(false).take(50)

	println("\nTop 50 similar movies for " + movieNameDict.get(movieID))

	for (r <- result) {
		val sim = r._1
		val pair = r._2
		// Display the similarity result that isn't the movie we're looking at
		var similarMovieID = pair._1

		if (similarMovieID == movieID) similarMovieID = pair._2

		println(movieNameDict.get(similarMovieID) + "\tscore: " + sim._1 + "\tstrength: " + sim._2)
	}

	sc.stop()
}