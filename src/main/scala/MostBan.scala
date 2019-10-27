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
      .collect()
      .sortWith(_._2 > _._2)

    spark.stop()
    for (i <- 0 to 4) {
        val hero_name = SparkMongoHelper.getHeroName(most_ban_hero(i)._1)
        println(s"${i + 1}th most banned hero is ${hero_name}. Banned ${most_ban_hero(i)._2} times.")
    }
  }

}