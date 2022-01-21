import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import scala.io.StdIn
import scala.io.Source

object big_data_campaign extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]", "app")
  val load_file = sc.textFile("C:\\data\\bigdatacampaigndata-201014-183159.csv")
  val mapped_input = load_file.map(x => (x.split(",")(10).toFloat, x.split(",")(0)))
  val final_mapped = mapped_input.flatMapValues(x => x.split(" ")).map(x => (x._2.toLowerCase(), x._1))
  val total = final_mapped.reduceByKey((x, y) => x + y).sortBy(x => x._2, ascending = false)
  total.collect().foreach(println)


}
