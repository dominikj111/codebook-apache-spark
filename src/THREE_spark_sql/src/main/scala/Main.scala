
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

import RDDImplicits._, FlowImplicits._, LocalSparkUtils._

import grizzled.slf4j._

import scala.math.{sqrt,Pi}

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
    val score: (Int, Int, Int) => Double = (xx, yy, xy) => {
        // val denominator = sqrt(xx) * sqrt(yy)
        // if(denominator == 0) denominator else xy / denominator
        (xx + yy).processAndGetResult[Double]( rr => if(rr == 0) rr else (Pi * rr + sqrt(xx)))
    }

    // TESTED !
    // val eval = (a: Int, b: Int) => { val rr = a * a + b * b; if(rr == 0) rr else (((scala.math.Pi * rr) + a)) }

    // interest : todo : save to ...
    // { for (i <- 0 to 1000) yield for (j <- 0 to 1000) yield (i,j,eval(i,j)) }
    //     .flatMap(a => a).sortWith(_._3 < _._3).groupBy(a => a._3).map(a => (a._1, a._2.size)).filter(a => a._2 != 1)

    // { for (i <- 1 to 5) yield for (j <- 1 to 5) yield (i,j,eval(i,j)) }
    //     .flatMap(a => a).sortWith(_._3 < _._3).foreach(a => println(f"[${a._1}:${a._2}] ${a._3}%1.3f"))


    val aaMul_udf = udf[Int,Int](aaMul)
    val abMul_udf = udf[Int,Int,Int](abMul)
    val score_udf = udf[Double,Int,Int,Int](score)

    val restructuredSchema = joinedSchema.select(
            $"ms1.movieID".alias("movieid1"),
            $"ms2.movieID".alias("movieid2"),
            $"ms1.movieRating".alias("x"),
            $"ms2.movieRating".alias("y")
        )

        .withColumn("xx", aaMul_udf('x))
        .withColumn("yy", aaMul_udf('y))
        .withColumn("xy", abMul_udf('x,'y))

        .groupBy("movieid1","movieid2").agg(sum("xx").alias("sumXX"),sum("yy").alias("sumYY"),sum("xy").alias("sumXY"),count("*").alias("count2"))

        .withColumn("score", score_udf('sumXX,'sumYY,'sumXY))

        .where($"score" < 1000)

        // .groupBy("x","y", "score").agg(count(lit(1)).alias("groupCount"))


    // restructuredSchema.filter($"x" === 4 && $"y" === 4).show
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
