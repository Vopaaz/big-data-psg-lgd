import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._

object EasyBQJZW {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("TEST_JZW")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .getOrCreate()

  def main(args: Array[String]) {
    cost_time()
    most_buy()
    spark.stop()
  }

  def cost_time() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val duration = rdd
      .map(x => (x.get("duration").toString.toInt, 1)).reduce((x, y) => (x._1 + y._1, x._2 + y._2))
    println(s"Average cost of time is ${duration._1/(duration._2 * 60)} minutes")
  }

  def most_buy() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val most_buy = rdd
      .map(x => x.get("players")
      .asInstanceOf[ArrayList[org.bson.Document]])
      .map(_.toSeq)
      .flatMap(x => x.map(y => y))
      .map(x => List(x.get("item_0"), x.get("item_1"), x.get("item_2"), x.get("item_3"), x.get("item_4"), x.get("item_5")))
      .flatMap(x => x.map(y => (y.toString.toInt, 1)))
      .filter(x => x._1 != 0)
      .reduceByKey(_ + _)
      .reduce((x, y) => if(x._2 > y._2) x else y)
      println(s"Most bought item id is ${most_buy._1}")
  }

}