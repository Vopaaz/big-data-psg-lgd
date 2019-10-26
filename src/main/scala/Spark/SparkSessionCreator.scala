package Spark
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.bson.Document

import org.apache.log4j.Logger
import org.apache.log4j.Level

class SparkSessionCreator {
	val conf: SparkConfig = new SparkConfig("config.yml")
	def getSparkSession(funName: String, input: String, output: String): SparkSession = {
		var dbCollections = Array("matchResults", "professionalGames", "publicGames", "rankedGames")
	    if (!(dbCollections contains input)) {
	    	throw new IllegalArgumentException(
	    		input + " is not a valid collection name"
	      )
	    }

	    if (!(dbCollections contains output)) {
	    	throw new IllegalArgumentException(
	    		output + " is not a valid collection name"
	      )
	    }

	    val spark = SparkSession
	      .builder()
	      .master("local")
	      .appName(conf.getAppName(funName))
	      .config("spark.mongodb.input.uri", conf.getCollection(input))
	      .config("spark.mongodb.output.uri", conf.getCollection(output))
	      .getOrCreate()

	    return spark
  }
  def getHeroSparkSession(): SparkSession = {
  	val spark = SparkSession
	      .builder()
	      .master("local")
	      .appName("hero-retriever")
	      .config("spark.mongodb.input.uri", conf.getCollection("heros"))
	      .config("spark.mongodb.output.uri", conf.getCollection("heros"))
	      .getOrCreate()

	    return spark
  }
  def getItemSparkSession(): SparkSession = {
  	val spark = SparkSession
	      .builder()
	      .master("local")
	      .appName("item-retriever")
	      .config("spark.mongodb.input.uri", conf.getCollection("items"))
	      .config("spark.mongodb.output.uri", conf.getCollection("items"))
	      .getOrCreate()

	    return spark
  }
}