import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.bson.Document

import org.apache.log4j.Logger
import org.apache.log4j.Level
import Spark.SparkSessionCreator
import Spark.SparkMongoHelper

object HeroMostStats {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    hero_most_stats("kills", "rankedGames")
    hero_most_stats("assists", "rankedGames")
    hero_most_stats("deaths", "rankedGames")
    hero_most_stats("hero_healing", "rankedGames")

    hero_most_stats("kills", "publicGames")
    hero_most_stats("assists", "publicGames")
    hero_most_stats("deaths", "publicGames")
    hero_most_stats("hero_healing", "publicGames")

    hero_most_stats("kills", "professionalGames")
    hero_most_stats("assists", "professionalGames")
    hero_most_stats("deaths", "professionalGames")
    hero_most_stats("hero_healing", "professionalGames")
  }

  def hero_most_stats(stats: String, gameType:String) = {
    val spark: SparkSession = sessionCreator.getSparkSession("HeroMostStats", "matchResults", "matchResults")
    if (!(Array(
            "kills",
            "assists",
            "deaths",
            "hero_healing"
        ) contains stats)) {
      throw new IllegalArgumentException(
          stats + " is not a valid hero statistic."
      )
    }

    SparkMongoHelper.printGame(gameType)

    val rdd   = MongoSpark.load(spark.sparkContext)

    val games = rdd.filter(SparkMongoHelper.is_wanted_match_type(gameType))

    val hero_group = games
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .filter(x => (x.getInteger("hero_id") != null))
      .filter(x => (x.getInteger(stats) != null))
      .map(x => (x.getInteger("hero_id"), x.getInteger(stats)))
      .reduceByKey(_ + _)
      .collect()
      .sortWith(_._2 > _._2)
    spark.stop()
    for (i <- 0 to 4) {
        val hero_name = SparkMongoHelper.getHeroName(hero_group(i)._1)
        println(s"Hero who has most ${stats} is ${hero_name}. Has ${hero_group(i)._2} times.")
    }
  }
}