
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

import RDDImplicits._, FlowImplicits._, LocalSparkUtils._

import grizzled.slf4j._

import scala.math.sqrt

case class Movie(userID: Int, movieID: Int, movieRating: Int)

object Main extends App with Logging {

	val logout = Logger("com.sundogsoftware.spark.Main")

    val movieNameDict = loadDict[Int, String](
        path = "../../ml-100k/u.item",
        dataDelimiter = "\\|",
        keyIndex = 0,
        valueIndex = 1,
        keyTransform = _.trim().toInt,
        valueTransform = _.trim()
    )

    logout.info("Dictionary 'ml-100k/u.item' loaded")

	val spark = SparkSession
        .builder
        .appName("SparkSQL")
        .master("local[*]")
        .getOrCreate()

	val moviesData = spark.sparkContext.textFile("../../ml-100k/u.data.less")
        .map( _.split("\\t") )
        .map( x => Movie(x(0).trim().toInt, x(1).trim().toInt, x(2).trim().toInt) )

    import spark.implicits._
    val movieSchema = moviesData.toDS.cache

    logout.info("Movie Schema:")
    movieSchema.printSchema()

    // movieSchema.createOrReplaceTempView("movies1")
    // movieSchema.createOrReplaceTempView("movies2")

    // cause OutOfMemoryError: GC overhead limit exceeded
    // val joinedTable = spark.sql("""
    //     SELECT
    //         m1.movieID as movie1ID,
    //         m2.movieID as movie2ID,
    //         m1.movieRating as mr1,
    //         m2.movieRating as mr2
    //     FROM movies1 m1 JOIN movies2 m2 ON m1.movieID < m2.movieID
    // """)

    //////////////////
    // join testing //
    //////////////////

    // val employees = sc.parallelize(Array[(String, Option[Int])](
    //     ("Rafferty", Some(31)),
    //     ("Jones", Some(33)),
    //     ("Heisenberg", Some(33)),
    //     ("Robinson", Some(34)),
    //     ("Smith", Some(34)),
    //     ("Williams", null))
    // ).toDF("LastName", "DepartmentID")

    // val employees2 = employees.as("empl2")

    // employees.as("empl1").join(employees.as("empl2")).where($"empl1.LastName" !== $"empl2.LastName").show
    // employees.as("empl1").join(employees.as("empl2"),$"empl1.LastName" !== $"empl2.LastName").show
    // employees.join(employees2,employees("LastName") !== employees2("LastName")).show

    // it does working even without reduce data bundle !!!!
    val joinedSchema = movieSchema.as("ms1").join(movieSchema.as("ms2")).where($"ms1.movieID" < $"ms2.movieID").drop("userID")

    import org.apache.spark.sql.functions.{udf,sum,count,lit}

    val aaMul: Int => Int = a => a * a
    val abMul: (Int, Int) => Int = (a, b) => a * b
    val score: (Int, Int, Int) => Double = (sum_xx, sum_yy, sum_xy) => {
        // val denominator = sqrt(sum_xx) * sqrt(sum_yy)
        // if(denominator == 0) denominator else sum_xy / denominator
        (sqrt(sum_xx) * sqrt(sum_yy)).processAndGetResult[Double]( r => if(r == 0) r else sum_xy / r)
    }

    val aaMul_udf = udf[Int,Int](aaMul)
    val abMul_udf = udf[Int,Int,Int](abMul)
    val score_udf = udf[Double,Int,Int,Int](score)

    val restructuredSchema = joinedSchema.select(
            $"ms1.movieID".alias("movieid1"),
            $"ms2.movieID".alias("movieid2"),
            $"ms1.movieRating".alias("ratx1"),
            $"ms2.movieRating".alias("raty2")
        )

        .withColumn("xx", aaMul_udf('ratx1))
        .withColumn("yy", aaMul_udf('raty2))
        .withColumn("xy", abMul_udf('ratx1,'raty2))

        .groupBy("movieid1","movieid2").agg(sum("xx"),sum("yy"),sum("xy"),count(lit(1)).alias("numPairs"))

        .withColumn("score", score_udf('xx,'yy,'xy))

    restructuredSchema.show

    // df.join(df2, df.col("key") === df2.col("key")).show



    // schemaPeople.select("name").show()




    // schemaPeople.filter(schemaPeople("age") < 21).show()


    // schemaPeople.groupBy("age").count().show()


    // schemaPeople.select(schemaPeople("name"), schemaPeople("age") + 10).show()


    // teenagers.foreach(println)

    // schemaPeople.cache()

    // println("Here is our inferred schema:")
    // schemaPeople.printSchema()

    // println("Let's select the name column:")
    // schemaPeople.select("name").show()

    // println("Filter out anyone over 21:")
    // schemaPeople.filter(schemaPeople("age") < 21).show()

    // println("Group by age:")
    // schemaPeople.groupBy("age").count().show()

    // println("Make everyone 10 years older:")
    // schemaPeople.select(schemaPeople("name"), schemaPeople("age") + 10).show()

    spark.stop()
}
