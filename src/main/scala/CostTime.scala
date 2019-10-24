import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Mongo.MongoConfig

object CostTime {
  val conf: MongoConfig = new MongoConfig("config.yml")

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName(conf.getAppName())
      .config("spark.mongodb.input.uri", conf.getInput())
      .config("spark.mongodb.output.uri", conf.getOutput())
      .getOrCreate()

  def main(args: Array[String]) {
    cost_time()
    spark.stop()
  }

  def cost_time() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val duration = rdd
      .map(x => (x.get("duration").toString.toInt, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(s"Average cost of time is ${duration._1/(duration._2 * 60)} minutes")
  }

}