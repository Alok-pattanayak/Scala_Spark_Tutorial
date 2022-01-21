import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn
import scala.io.Source
// moving ratings
object movie_ratings extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "word_count")
  val load_file = sc.textFile("C:\\data\\moviedata-201008-180523.data")
  val mapped_input = load_file.map(x => x.split("\t")(2))
  //val ratings = mapped_input.map(x => (x,1))
  //val final_ratings = ratings.reduceByKey((x,y) => x+y)
  val final_ratings = mapped_input.countByValue
  final_ratings.foreach(println)

}
