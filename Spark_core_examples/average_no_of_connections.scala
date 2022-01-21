import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn
import scala.io.Source


object average_no_of_connections extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "word_count")
  val load_file = sc.textFile("C:\\data\\friendsdata-201008-180523.csv")
  val mapped_input = load_file.map(x => (x.split(",")(2).toInt, x.split(",")(3).toInt))
  val map_final = mapped_input.map(x => (x._1, (x._2, 1)))
  val totalsByAge = map_final.reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))
  val averageByAge = totalsByAge.map(x => (x._1, x._2._1 / x._2._2)).sortBy(x => x._2, ascending = false)
  //val result = averageByAge.collect()

  averageByAge.collect().foreach(println)

  //for(x <- result){
  //  println(x)
  //}

}
