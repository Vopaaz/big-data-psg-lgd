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

object CostTime {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    cost_time("publicGames")
    cost_time("rankedGames")
    cost_time("professionalGames")
  }

  def cost_time(gameType: String) {
    val spark: SparkSession = sessionCreator.getSparkSession("CostTime", "matchResults", "matchResults")
    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

    val games = rdd.filter(SparkMongoHelper.is_wanted_match_type(gameType))
    val duration = games.filter(x => x.getInteger("human_players") == 10)
      .map(x => (x.get("duration").toString.toInt, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(s"Average cost of time is ${duration._1/(duration._2 * 60)} minutes")
    spark.stop()
  }

}