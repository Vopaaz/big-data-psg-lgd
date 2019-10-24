import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Mongo.MongoConfig

object MostPick {
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
    most_pick()
    spark.stop()
  }

  def most_pick() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_pick_hero = rdd
      .map(x => x.get("players").asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
    println("The most picked hero with id: " + most_pick_hero)
  }

}