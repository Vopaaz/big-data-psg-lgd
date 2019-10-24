import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Mongo.MongoConfig

object BadManner {
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
    bad_manner()
    spark.stop()
  }

  def bad_manner() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val players = rdd
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
    val afk_players = players.filter(x => if(x.getInteger("leaver_status") == null) false
      else (x.getInteger("leaver_status") <= 4 && x.getInteger("leaver_status") >= 2))
      .filter(x => x.getInteger("leaver_status") <= 4)
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
      println("The most bad manner hero has id: " + afk_players)
  }

}