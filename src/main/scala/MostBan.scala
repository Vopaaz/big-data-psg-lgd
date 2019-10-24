import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Mongo.MongoConfig

object MostBan {
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
    most_ban()
    spark.stop()
  }

  def most_ban() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_ban_hero = rdd
      .filter(x => x.getInteger("leagueid") != 0)
      .filter(x => x.get("picks_bans") != null)
      .map(x => x.get("picks_bans").asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .filter(x => !x.getBoolean("is_pick"))
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
    println("The most banned hero with id: " + most_ban_hero)
  }

}