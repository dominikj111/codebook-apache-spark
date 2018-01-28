
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.sql._
import org.apache.spark.SparkContext._

import RDDImplicits._

import grizzled.slf4j._

case class Person(ID: Int, name: String, age: Int, numFriends: Int)

object Main extends App with Logging {

	val logout = Logger("com.sundogsoftware.spark.Main")

	val spark = SparkSession
		.builder
		.appName("SparkSQL")
		.master("local[*]")
		.getOrCreate()

	val lines = spark.sparkContext.textFile("../../SparkScala/fakefriends.csv")
    val people = lines.map( _.split(",") ).map( x => Person(x(0).toInt, x(1).toString, x(2).toInt, x(3).toInt) )

    // people.asInstanceOf[RDD[Person]].printn(10)

    import spark.implicits._
    val schemaPeople = people.toDS

    // schemaPeople.printSchema()
    // schemaPeople.createOrReplaceTempView("people")

    // val teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19").collect()
    // teenagers.foreach(println)

    schemaPeople.cache()

    println("Here is our inferred schema:")
    schemaPeople.printSchema()

    println("Let's select the name column:")
    schemaPeople.select("name").show()

    println("Filter out anyone over 21:")
    schemaPeople.filter(schemaPeople("age") < 21).show()

    println("Group by age:")
    schemaPeople.groupBy("age").count().show()

    println("Make everyone 10 years older:")
    schemaPeople.select(schemaPeople("name"), schemaPeople("age") + 10).show()

    spark.stop()
}
