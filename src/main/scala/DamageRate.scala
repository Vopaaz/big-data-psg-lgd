import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import Spark.SparkSessionCreator
import Spark.SparkMongoHelper
import java.io._

object DamageRate {

  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    gold_to_damage("publicGames")
    gold_to_damage("rankedGames")
    gold_to_damage("professionalGames")
  }

  def gold_to_damage(gameType:String) {

    val spark: SparkSession = sessionCreator.getSparkSession("GoldDamageRate", "matchResults", "matchResults")

    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

    val games = rdd.filter(SparkMongoHelper.is_wanted_match_type(gameType))

    val players = games
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))

    val gold_damage_per_player = players
      .map(x => (x.getInteger("hero_id"),
        (x.getInteger("hero_damage").toDouble,
        (x.getInteger("gold_spent").toLong + x.getInteger("gold").toLong))))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(x => (x._2._1 > 1) && (x._2._2 > 1))
      .map(x => (x._1, x._2._1 / x._2._2))
      .collect()

    val sorted_results = gold_damage_per_player
      .sortWith((a, b) => (a._2 > b._2))

    spark.stop()
    val pw = new PrintWriter(new File("result/damage_rate_result.txt" ))

    for(i <- 0 to 19) {
      val hero_name = SparkMongoHelper.getHeroName(sorted_results(i)._1)
      pw.write(s"Hero who has top ${i+1} damage rate is ${hero_name}. Damage rate: ${sorted_results(i)._2}\n")
      println(s"Hero who has top ${i+1} damage rate is ${hero_name}. Damage rate: ${sorted_results(i)._2}")
    }

    println()

    for(i <- (sorted_results.size-20) to (sorted_results.size-1)) {
      val hero_name = SparkMongoHelper.getHeroName(sorted_results(i)._1)
      pw.write(s"Hero who has bottom ${sorted_results.size - i}th damage rate is ${hero_name}. Damage rate: ${sorted_results(i)._2}\n")
      println(s"Hero who has bottom ${sorted_results.size - i}th damage rate is ${hero_name}. Damage rate: ${sorted_results(i)._2}")
    }

    pw.close
  }
}