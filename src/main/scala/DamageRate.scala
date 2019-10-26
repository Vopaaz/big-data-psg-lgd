import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._

object DamageRate {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .getOrCreate()

  def main(args: Array[String]) {
    gold_to_damage()
    spark.stop()
  }

  def gold_to_damage() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val players = rdd
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
    val gold_damage_per_player = players
      .filter(x => if(x.getInteger("leaver_status") == null) false
      else (x.getInteger("leaver_status") <= 4 && x.getInteger("leaver_status") >= 2))
      .filter(x => x.getInteger("leaver_status") <= 4)
      .map(x => (x.getInteger("hero_id"),
        (x.getInteger("hero_damage").toDouble, (x.getInteger("gold_spent") + x.getInteger("gold")))))
      .reduceByKey((a, b) => (a._1 + b._1, a._2 + b._2))
      .filter(x => (x._2._1 > 1) && (x._2._2 > 1))
      .map(x => (x._1, x._2._1 / x._2._2))
      .collect()
    val sorted_results = gold_damage_per_player.sortWith((a, b) => (a._2 > b._2))
      .foreach(println)
  }
}