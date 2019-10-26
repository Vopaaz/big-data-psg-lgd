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
    most_pick()
  }

  def most_pick() {
    val spark: SparkSession = sessionCreator.getSparkSession("MostUsedItem", "matchResults", "matchResults")
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_pick_hero = rdd
      .map(x => x.get("players").asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => (x.getInteger("hero_id"), 1))
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)

    spark.stop()

    val hero_name = SparkMongoHelper.getHeroName(most_pick_hero._1)
    println("The most picked hero is " + hero_name)
  }

}