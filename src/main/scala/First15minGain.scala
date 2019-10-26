import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.bson.Document
import Spark.SparkSessionCreator
import org.apache.log4j.Logger
import org.apache.log4j.Level

object First15minGain {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    val res_XP_ranked = first_15min_gain("rankedGames", "XP")
    val res_gold_ranked = first_15min_gain("rankedGames", "gold")

    val res_XP_public = first_15min_gain("publicGames", "XP")
    val res_gold_public = first_15min_gain("publicGames", "gold")

    val res_XP_professional = first_15min_gain("professionalGames", "XP")
    val res_gold_professional = first_15min_gain("professionalGames", "gold")
  }

  def first_15min_gain(gameType: String, gold_or_XP: String): String = {
    gameType match {
      case "rankedGames" => println("In ranked games:")
      case "publicGames" => println("In public games games:")
      case "professionalGames" => println("In professional games games:")
    }
    val spark: SparkSession = sessionCreator.getSparkSession("First15minGain", gameType, gameType)
    val rdd   = MongoSpark.load(spark.sparkContext)

    val combatlog = rdd
      .flatMap(
          x =>
            x.get("combatlog")
              .asInstanceOf[ArrayList[Document]]
      )

    val valid_events = combatlog
      .filter(
          x => x.get("type") == gold_or_XP
      )
      .filter(
          x => x.getDouble("time") <= 15 * 60
      )
      .filter(x=> x.get(if(gold_or_XP == "gold") "change" else "xp" ) != null)



    val hero_gain = valid_events
      .groupBy(x => x.get("target"))
      .map(
          x =>
            Tuple2(
                x._1,
                x._2.aggregate(0d)(
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

    val hero_gain_map: Map[Object, Double] =
      hero_gain.collect().toMap[Object, Double]

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
    println(s"The hero who gains most ${gold_or_XP} in first 15 mins is ${result}")

    return result
  }
}