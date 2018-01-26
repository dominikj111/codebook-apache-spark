package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd._
import org.apache.spark.util.LongAccumulator
import grizzled.slf4j._
import scala.collection.mutable.ArrayBuffer

import scala.util.control.Breaks._

import scala.io.Codec
import scala.io.Source
import java.nio.charset.CodingErrorAction

import scala.math.max
import scala.math.min

import enumeratum._

// 'with Product with Serializable' traits need to be placed for support Colllection.reduce function => println(Colors.values.reduce( (a,b) => Colors.brightest(a,b) ))
// 'with Serializable' need to be placed for support RDD computation

sealed abstract class Colors(val brightness: Int) extends EnumEntry with Serializable {}

object Colors extends Enum[Colors] {

	case object WHITE extends Colors(3)
	case object GRAY  extends Colors(2)
	case object BLACK extends Colors(1)

	val values = findValues

	def darkest(a: Colors, b: Colors): Colors = if(a.brightness < b.brightness) a else b
	def brightest(a: Colors, b: Colors): Colors = if(a.brightness > b.brightness) a else b
}

object RDDImplicits {
	implicit class RichRDDT2[A,B](rdd: org.apache.spark.rdd.RDD[(A,B)]) {
		def flip = { rdd.map(element => (element._2, element._1)) }
		def print() = { rdd.collect.foreach(println) }
		def printn(n: Int) = { rdd.collect.slice(0, n).foreach(println) }
		def size = { rdd.map(x => (1,1)).reduceByKey(_ + _).collect.asInstanceOf[Array[Tuple2[Int,Int]]](0)._2 }
	}
	implicit class RichRDDT1[A](rdd: org.apache.spark.rdd.RDD[(A)]) {
		def print() = { rdd.collect.foreach(println) }
		def printn(n: Int) = { rdd.collect.slice(0, n).foreach(println) }
		def size = { rdd.map(x => (1,1)).reduceByKey(_ + _).collect.asInstanceOf[Array[Tuple2[Int,Int]]](0)._2 }
	}
}

object FlowImplicits {
	implicit class AnyExtensions[A](x: A) {
		def when(f: A => Boolean)(action: A => Unit): Unit = if (f(x)) action(x)
		def processAndGet(f: A => Unit): A = { f(x) ; x }
		// def whenElse[B](f: A => Boolean)(action: A => B)(alterAction: A => B): B = if (f(x)) action(x) else alterAction(x)
	}
	implicit def ElvisOperator[A](right: A) { // NO TESTED
		def ?:(left: A) = if (left == null) right else left
	}
}

object StringImplicits {
	implicit class Editing(x: String) {
		def removeAll(toDelete: String): String = x.replace(toDelete,"")
	}
}

object graphHero extends Logging {

	import Colors._

	val logout = Logger("com.sundogsoftware.spark.graphHero")

	def loadDict[A, B](
		path: String,
		dataDelimiter: String,
		keyIndex: Int,
		valueIndex: Int,
		keyTransform: String => A,
		valueTransform: String => B ): Map[A, B] = {

			implicit val codec = Codec("UTF-8")
			codec.onMalformedInput(CodingErrorAction.REPLACE)
			codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

			var dict: Map[A, B] = Map()

			import FlowImplicits._, scala.math.max

			Source.fromFile(path).getLines().foreach(
				_.split(dataDelimiter)
					.when( _.length > max(keyIndex, valueIndex) )( x => keyTransform(x(keyIndex)) -> valueTransform(x(valueIndex)) )
			)

			return dict
	}

	type BFSData = (Array[Int], Int, Colors)
	type BFSNode = (Int, BFSData)

	var hitCounter: Option[LongAccumulator] = None

	def lookAround(initNode: BFSNode, targetId: Int): Array[BFSNode] = {

		val characterID: Int = initNode._1
		val data: BFSData = initNode._2
		val connections: Array[Int] = data._1
		val distance: Int = data._2

		var color: Colors = data._3

		val result: ArrayBuffer[BFSNode] = ArrayBuffer()

		if(color == Colors.GRAY){

			color = Colors.BLACK

			for(connection <- connections){

				assert(distance < Int.MaxValue, "Current distance has to be less then Int.MaxValue, white nodes are not processed")

				if(hitCounter.isDefined && targetId == connection) hitCounter.get.add(1)
				result += (connection, (Array[Int](), distance + 1, Colors.GRAY).asInstanceOf[BFSData]).asInstanceOf[BFSNode]
			}
		}

		// val anotherOption: BFSNode = ( characterID, ( connections, distance, color ))
		// result += anotherOption

		result += ( characterID, ( connections.map(_.toInt), distance, color ).asInstanceOf[BFSData]).asInstanceOf[BFSNode]

		result.toArray
	}

	def nodeReduce(A: BFSData, B: BFSData): BFSData = {

		val connections: Array[Int] = (ArrayBuffer() ++= A._1 ++= B._1).toSet.toArray
		val minDistance: Int = Array(A._2, B._2).reduce(_ min _)
		val darkestColor: Colors = Colors.darkest(A._3, B._3)

		(connections, minDistance, darkestColor).asInstanceOf[BFSData]
	}

	def main(args: Array[String]) {

		import RDDImplicits.RichRDDT2, RDDImplicits.RichRDDT1, StringImplicits._, FlowImplicits._

		val sc = new SparkContext("local[*]", "graphHero")
		val marvelGraphFile = sc.textFile("../../SparkScala/Marvel-graph.txt")

		// do not need now
		// val nameDict = sc.broadcast(
		//     loadDict(
		//         path = "../../SparkScala/Marvel-names.txt",
		//         dataDelimiter = "\"",
		//         keyIndex = 0,
		//         valueIndex = 1,
		//         keyTransform = _.trim().toInt,
		//         valueTransform = _.removeAll("\"").trim()
		//     )
		// )

		hitCounter = Some(sc.longAccumulator("Hit Counter"))

		val startHeroId = 2898 // "KRONOS"
		val targetHeroId = 1917 // "FLASH/BARRY ALLEN/BU"

		var distance: Option[Int] = None
		var color: Option[Colors] = None

		var iterationRDD = marvelGraphFile.map( _.split("\\D+") )
			.map( x => (
					x(0).trim().toInt.processAndGet(
						x =>
							if (x == startHeroId) { distance = Some(0); color = Some(Colors.GRAY) }
							else { distance = Some(Int.MaxValue); color = Some(Colors.WHITE) } ),
					(
						x.slice(1, x.length).map( _.toInt ), // "[" + x.slice(1, x.length).mkString(":") + "]"
						distance.get,
						color.get
					)
				)
			)

		// val kronosKey = 2898
		// val kronosValue = iterationRDD.lookup(kronosKey)
		// kronosValue(0)._1.foreach(println)
		// lookAround((kronosKey, kronosValue(0)).asInstanceOf[BFSNode], targetHeroId).foreach(println)

		// .printn(10)

		// val mapped = iterationRDD.flatMap(lookAround(_,targetHeroId)).reduceByKey(nodeReduce(_,_))
		// mapped.print()

		// mapped.reduceByKey(nodeReduce(_,_)).printn(10)

		breakable {
			for (iteration <- 1 to 10) {

				println("Running BFS Iteration# " + iteration)

				val mapped = iterationRDD.flatMap(lookAround(_,targetHeroId))

				// Note that mapped.count() action here forces the RDD to be evaluated, and
				// that's the only reason our accumulator is actually updated.
				val mappedCount = mapped.count()
				println("Processing " + mappedCount + " values.")

				assert(mappedCount == mapped.size, "Size doesn't fit correct value (calculation is wrong)")

				if (hitCounter.isDefined && hitCounter.get.value > 0) {
					println("Hit the target character! From " + hitCounter.get.value + " different direction(s).")
					break
				}

				iterationRDD = mapped.reduceByKey(nodeReduce(_,_))
			}
		}

		sc.stop()
	}
}
