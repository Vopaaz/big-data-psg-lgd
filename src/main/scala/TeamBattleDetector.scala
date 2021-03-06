import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.ArrayList
import scala.collection.mutable.PriorityQueue
import Spark.SparkSessionCreator
import Spark.SparkMongoHelper
import java.io._

object TeamBattleDetector {

  val sessionCreator: SparkSessionCreator = new SparkSessionCreator()

  def main(args: Array[String]) {
    team_battles_generator("publicGames")
    team_battles_generator("rankedGames")
    team_battles_generator("professionalGames")
  }

  def team_battles_generator(gameType:String) {
    val spark: SparkSession = sessionCreator.getSparkSession("First15minGain", gameType, gameType)

    val rdd = MongoSpark.load(spark.sparkContext)

    SparkMongoHelper.printGame(gameType)

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

    println("The time of the first team battle is " + (first_battle._1.toDouble / (first_battle._2 * 60)) + " mins\n")
    println("The average number of team battles is " + (battle_times._1.toDouble / battle_times._2) + " s")
    spark.stop()

    val pw = new PrintWriter(new File("result/team_battle_result.txt" ))
    pw.write("The time of the first team battle is " + (first_battle._1.toDouble / first_battle._2) + " mins\n")
    pw.write("The average number of team battles is " + (battle_times._1.toDouble / battle_times._2) + " s\n")
    pw.close

  }

  def team_battles_detector(death_times: Seq[Double]) : (Seq[Double]) = {
      var res = List[Double]()
      var pq = PriorityQueue.empty[Double]
      for(time <- death_times) {
        pq.enqueue(time)
        if(!pq.isEmpty && (pq.last + 180) < time) {
          if(pq.size() >= 4) {
            res = res :+ pq.head
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