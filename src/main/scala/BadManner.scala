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
    bad_manner()
  }

  def bad_manner() {
    val spark: SparkSession = sessionCreator.getSparkSession("BadManner", "matchResults", "matchResults")
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
    
    spark.stop()
    val hero_name = SparkMongoHelper.getHeroName(afk_players._1)
    println(s"The most bad manner hero is: ${hero_name}. AFK ${afk_players._2} times") 
  }

}