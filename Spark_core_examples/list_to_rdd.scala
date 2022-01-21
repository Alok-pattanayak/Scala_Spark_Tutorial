
import org.apache.spark.SparkContext
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD

import scala.io.StdIn
import scala.io.Source
//get the count of warn and error
object list_to_rdd extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "biglog")
  val mylist = List("ERROR: Thu Jun 04 10:37:51 BST 2015",
  "WARN: Sun Nov 06 10:37:51 GMT 2016",
  "WARN: Mon Aug 29 10:37:51 BST 2016",
  "ERROR: Thu Dec 10 10:37:51 GMT 2015",
  "ERROR: Fri Dec 26 10:37:51 GMT 2014",
  "ERROR: Thu Feb 02 10:37:51 GMT 2017")
  val original_rdd: RDD[String] = sc.parallelize(mylist)
  val pair_original_rdd: RDD[(String, Int)] = original_rdd.map(x => {
    val columns = x.split(":")
    val level = columns(0)
    (level,1)
  })
  val result_rdd: RDD[(String, Int)] = pair_original_rdd.reduceByKey((x, y) => x+y)
  result_rdd.collect().foreach(println)


}
