import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Mongo.MongoConfig

object MostBuy {
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
    most_buy()
    spark.stop()
  }

  def most_buy() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_buy = rdd
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => List(x.get("item_0"), x.get("item_1"), x.get("item_2"), x.get("item_3"), x.get("item_4"), x.get("item_5")))
      .flatMap(x => x.map(y => (y.toString.toInt, 1)))
      .filter(x => x._1 != 0)
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
      println(s"Most bought item id is ${most_buy._1}")
  }

}