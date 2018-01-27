
import org.apache.spark.rdd.RDD

object RDDImplicits {
	implicit class RichRDDT2[A,B](rdd: RDD[(A,B)]) {
		def flip = { rdd.map(element => (element._2, element._1)) }
		def print() = { rdd.collect.foreach(println) }
		def printn(n: Int) = { rdd.collect.slice(0, n).foreach(println) }
		def size = { rdd.map(x => (1,1)).reduceByKey(_ + _).collect.asInstanceOf[Array[Tuple2[Int,Int]]](0)._2 }
	}
	implicit class RichRDDT1[A](rdd: RDD[(A)]) {
		def print() = { rdd.collect.foreach(println) }
		def printn(n: Int) = { rdd.collect.slice(0, n).foreach(println) }
		def size = { rdd.map(x => (1,1)).reduceByKey(_ + _).collect.asInstanceOf[Array[Tuple2[Int,Int]]](0)._2 }
	}
}
