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
      .flatMap(
          x =>
            x.get("players")
              .asInstanceOf[ArrayList[Document]]
      )
      .groupBy(x => x.getInteger("hero_id"))

    val hero_stats = hero_group
      .map(
          x =>
            Tuple3(
                x._1,
                x._2.count(x => true),
                x._2.aggregate(0)(
                    (acc, item) => acc + item.getInteger(stats),
                    (acc1, acc2) => acc1 + acc2
                )
            )
      )
      .map(
          x =>
            Tuple2(
                x._1,
                x._3 / x._2
            )
      )

    val result = hero_stats.reduce(
        (x, y) => if (x._2 > y._2) x else y
    )
    spark.stop()
    val hero_name = SparkMongoHelper.getHeroName(result._1)
    println(s"Hero who has most ${stats} is ${hero_name}. Has ${result._2} times.")
  }
}