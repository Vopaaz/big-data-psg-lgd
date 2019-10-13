import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._

object EasyBQ {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .getOrCreate()

  def main(args: Array[String]) {
    // val rdd = MongoSpark.load(spark.sparkContext)
    // // print(typeOf(rdd))
    // // val prof = rdd.filter(x => x.getInteger("leagueid") != 0 )
    // // val public = rdd.filter(x => (x.getInteger("game_mode") == 1 || x.getInteger("game_mode")  == 3) && x.getInteger("leagueid") == 0)
    // val rank = rdd.filter(x => (x.getInteger("game_mode") == 22))
    // val players = rdd.map(x => x.get("players").asInstanceOf[ArrayList[org.bson.Document]])
    // // // println(prof.count())
    // // // println(public.count())
    // // // println(rank.count())
    // val hero_ids = players.map(_.toSeq).flatMap(x => x.map(y => y)).map(x => x.getInteger("hero_id"))
    // println("max is " + hero_ids.max())
    // println("min is " + hero_ids.min())
    bad_manner()
    most_pick()
    most_ban()
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

  def most_pick() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_pick_hero = rdd
      .map(x => x.get("players").asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
    println("The most picked hero with id: " + most_pick_hero)
  }

  def most_ban() {
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
    println("The most banned hero with id: " + most_ban_hero)
  }

}