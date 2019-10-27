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

object BadManner {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    bad_manner("publicGames")
    bad_manner("rankedGames")
    bad_manner("professionalGames")
  }

  def bad_manner(gameType: String) {
    val spark: SparkSession = sessionCreator.getSparkSession("BadManner", "matchResults", "matchResults")
    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

    val games = rdd.filter(SparkMongoHelper.is_wanted_match_type(gameType))

    val players = games.filter(x => x.getInteger("human_players") == 10)
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))

    val afk_players = players.filter(x => if(x.getInteger("leaver_status") == null) false
      else (x.getInteger("leaver_status") <= 4 && x.getInteger("leaver_status") >= 2))
      .filter(x => x.getInteger("leaver_status") <= 4)

    if (afk_players.take(1).length == 0 ) {
      println("No AFK players.")
      spark.stop()
      return
    }
    val afk_players_count = afk_players.map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)

    spark.stop()
    val hero_name = SparkMongoHelper.getHeroName(afk_players_count._1)
    println(s"The most bad manner hero is: ${hero_name}. AFK ${afk_players_count._2} times")
  }

}