package Spark
import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.util.ArrayList
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.bson.Document

object SparkMongoHelper{
	val sessionCreator: SparkSessionCreator = new SparkSessionCreator()
	def getHeroName(id: Int):String =  {
		val sparkHero: SparkSession = sessionCreator.getHeroSparkSession()
	    val rdd = MongoSpark.load(sparkHero.sparkContext)
	    val hero = rdd.filter(x => x.get("id") == id).first().get("name")
	    sparkHero.stop()
	    return hero.toString()
	}
	def getItemName(id: Int):String = {
		val sparkItem: SparkSession = sessionCreator.getItemSparkSession()
	    val rdd = MongoSpark.load(sparkItem.sparkContext)
	    val item = rdd.filter(x => x.get("id") == id).first().get("name")

	    sparkItem.stop()
	    return item.toString()
	}

	def is_professional(x: Document): Boolean = {
      return x.getInteger("leagueid") != 0
    }

    def is_ranked(x: Document): Boolean = {
      return x.getInteger("lobby_type") == 7
    }

    def is_public(x: Document): Boolean = {
      return x.getInteger("lobby_type") == 0
    }

	def is_wanted_match_type(gameType: String): Document => Boolean = gameType match {
      case "professionalGames" => is_professional
      case "publicGames"       => is_public
      case "rankedGames"       => is_ranked
    }

    def printGame(gameType: String) = gameType match {
      case "rankedGames" => println("In ranked games:")
      case "publicGames" => println("In public games games:")
      case "professionalGames" => println("In professional games games:")
    }
}