import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn
import scala.io.Source

object broadcast_example extends App {

  def boring_words(): Set[String] = {
    var boring_words: Set[String] = Set()
    val lines = Source.fromFile("C:\\data\\boringwords.txt")
    val data = lines.getLines()
    for (line <- data) {
      boring_words += line
    }
    boring_words
  }

  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "broadcast")
  var name_set = sc.broadcast(boring_words())
  val load_file = sc.textFile("C:\\data\\bigdatacampaigndata-201014-183159.csv")
  val mapped_input = load_file.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val final_mapped = mapped_input.flatMapValues(x => x.split(" ")).map(x => (x._2.toLowerCase(), x._1))
  val filter_data = final_mapped.filter(x => !name_set.value(x._1))
  val total = filter_data.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false)
  total.collect().foreach(println)

}
