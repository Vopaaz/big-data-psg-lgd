package EasyBQ

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

object EasyBQ123 {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    val rankedGames = new TypeOfGame("rankedGames")
    println(rankedGames.first_15min_gain("XP"))
    // println(rankedGames.hero_having_most_stats("kills"))
  }
}

class TypeOfGame(val collection: String) {

  def get_spark_session(match_result: Boolean = false): SparkSession = {
    if (!(Array(
            "matchResults",
            "professionalGames",
            "publicGames",
            "rankedGames"
        ) contains collection)) {
      throw new IllegalArgumentException(
          collection + " is not a valid collection name"
      )
    }

    val spark = SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .config(
          "spark.mongodb.input.uri",
          "mongodb://127.0.0.1/dota2." + (if (match_result) "matchResults"
                                          else collection)
      )
      .config(
          "spark.mongodb.output.uri",
          "mongodb://127.0.0.1/dota2." + (if (match_result) "matchResults"
                                          else collection)
      )
      .getOrCreate()

    return spark
  }

  def first_15min_gain(gold_or_XP: String): String = {
    val spark = get_spark_session()
    val rdd   = MongoSpark.load(spark.sparkContext)

    val combatlogs = rdd
      .flatMap(
          x =>
            x.get("combatlog")
              .asInstanceOf[ArrayList[Document]]
      )

    val valid_events = combatlogs
      .filter(
          x => x.get("type") == gold_or_XP
      )
      .filter(
          x => x.getDouble("time") <= 15 * 60
      )

    val hero_gain = valid_events
      .groupBy(x => x.get("target"))
      .map(
          x =>
            Tuple2(
                x._1,
                x._2.aggregate(0D)(
                    (acc, item) =>
                      acc +
                        item.getInteger(
                            if (gold_or_XP == "gold") "change" else "xp"
                        ),
                    (acc1, acc2) => acc1 + acc2
                )
            )
      )

    val hero_game_count = rdd
      .flatMap(
          x =>
            x.get("info")
              .asInstanceOf[Document]
              .get("player_info")
              .asInstanceOf[ArrayList[Document]]
      )
      .map(x => x.get("hero_name"))
      .countByValue()

    // Fixme: Int may overflow when using large dataset
    val hero_gain_map: Map[Object, Double] = hero_gain.collect().toMap[Object, Double]

    var result: String = ""
    var max: Double    = 0
    for (key <- hero_gain_map.keys) {
      var temp = hero_gain_map(key) / hero_game_count(key).toDouble

      if (temp > max) {
        max = temp
        result = key.toString()
      }
    }

    spark.stop()
    return result
  }

  def hero_having_most_stats(stats: String): String = {
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

    val spark = get_spark_session(match_result = true)
    val rdd   = MongoSpark.load(spark.sparkContext)

    def is_professional(x: Document): Boolean = {
      return x.getInteger("leagueid") != 0
    }

    def is_ranked(x: Document): Boolean = {
      return x.getInteger("lobby_type") == 7
    }

    def is_public(x: Document): Boolean = {
      return x.getInteger("lobby_type") == 0
    }

    val is_wanted_match_type: Document => Boolean = collection match {
      case "professionalGames" => is_professional
      case "publicGames"       => is_public
      case "rankedGames"       => is_ranked
    }

    val games = rdd.filter(is_wanted_match_type)

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
    return result._1.toString()
  }
}
