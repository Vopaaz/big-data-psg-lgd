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

object MostPick {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    most_pick("publicGames")
    most_pick("rankedGames")
    most_pick("professionalGames")
  }

  def most_pick(gameType: String) {
    val spark: SparkSession = sessionCreator.getSparkSession("MostUsedItem", "matchResults", "matchResults")
    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

    val games = rdd.filter(SparkMongoHelper.is_wanted_match_type(gameType)).filter(x => x.getInteger("human_players") == 10)
    val most_pick_hero = games
      .map(x => x.get("players").asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)

    spark.stop()

    val hero_name = SparkMongoHelper.getHeroName(most_pick_hero._1)
    println(s"The most picked hero is ${hero_name}. Picked ${most_pick_hero._2} times")
  }

}