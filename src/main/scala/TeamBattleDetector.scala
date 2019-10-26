import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.ArrayList
import scala.collection.mutable.PriorityQueue

object TeamBattleDetector {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dota2.rankedGames")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dota2.rankedGames")
      .getOrCreate()

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    team_battles_generator()
    spark.stop()
  }

  def team_battles_generator() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val team_battles = rdd
      .map(x => (x.get("combatlog").asInstanceOf[ArrayList[org.bson.Document]].toSeq))
      // split the players from two teams
      .map(x => x.filter(y => y.getString("type").equals("death") && y.getString("target").startsWith("npc_dota_hero")))
      // map relation ship to a value, 1 for win, 0 for lose
      .map(x => team_battles_detector(x.map(y => y.getDouble("time").toDouble)))
      .filter(x => x.length != 0)
      .cache()
    val first_battle = team_battles
      .map(x => x.head)
      .map(x => (x, 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    val battle_times = team_battles
      .map(x => x.length)
      .map(x => (x, 1))
      .reduce((a, b) => (a._1 + b._1, a._2 + b._2))
    println("The time of the first team battle is " + (first_battle._1.toDouble / first_battle._2))
    println("The average number of team battles is " + (battle_times._1.toDouble / battle_times._2))

  }

  def team_battles_detector(death_times: Seq[Double]) : (Seq[Double]) = {
      var res = List[Double]()
      var pq = PriorityQueue.empty[Double]
      for(time <- death_times) {
        pq.enqueue(time)
        if(!pq.isEmpty && (pq.last + 180) < time) {
          if(pq.size() >= 4) {
            res = pq.head :: res
          }
          pq = PriorityQueue.empty[Double]
        }
        while(!pq.isEmpty && (pq.head + 180) < time) {
          pq.dequeue()
        }
        pq.enqueue(time)
      }
      res
  }

}