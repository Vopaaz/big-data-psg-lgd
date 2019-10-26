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
}