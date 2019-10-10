package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._

import org.apache.log4j.Logger
import org.apache.log4j.Level

object Hello extends App {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("com").setLevel(Level.OFF)

  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Test")
    .config("spark.mongodb.input.uri", "mongodb://localhost/dota2.matchResults")
    .config(
        "spark.mongodb.output.uri",
        "mongodb://localhost/dota2.matchResults"
    )
    .getOrCreate()
  val rdd = MongoSpark.load(spark)

  rdd.show()

  spark.stop()
}
