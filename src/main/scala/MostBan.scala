import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Spark.SparkSessionCreator
import Spark.SparkMongoHelper
import org.apache.log4j.Logger
import org.apache.log4j.Level

object MostBan {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    most_ban()
  }

  def most_ban() {
    val spark: SparkSession = sessionCreator.getSparkSession("MostUsedItem", "matchResults", "matchResults")
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

    spark.stop()
    val hero_name = SparkMongoHelper.getHeroName(most_ban_hero._1)
    println(s"The most banned hero is ${hero_name}. Banned ${most_ban_hero._2} times")
  }

}