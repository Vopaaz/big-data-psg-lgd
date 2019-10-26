import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import Spark.SparkSessionCreator
import Spark.SparkMongoHelper
import org.bson.Document
import scala.collection.immutable.ListMap

object MostUsedItem {
  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    most_used_item("publicGames")
    most_used_item("rankedGames")
    most_used_item("professionalGames")
  }

  def most_used_item(gameType: String) = {
  val spark: SparkSession = sessionCreator.getSparkSession("MostUsedItem", gameType, gameType)
    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

    val combatlog = rdd
      .flatMap(
          x =>
            x.get("combatlog")
              .asInstanceOf[ArrayList[Document]]
      )

    val use_count = combatlog
      .filter(
          x => x.get("type") == "item"
      )
      .map(x => x.get("item"))
      .countByValue()
      .toSeq
      .sortWith(_._2 > _._2)

      for (i <- 0 to 4) {
        println(s"${i + 1}th most used item is ${use_count(i)._1}. Used ${use_count(i)._2} times.")
      }
      spark.stop()
  }

}