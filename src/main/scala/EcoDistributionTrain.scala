import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import java.util.ArrayList
object EcoDistributionTrain {

  val spark: SparkSession =
    SparkSession
      .builder()
      .master("local")
      .appName("Test")
      .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/dota2.matchResults")
      .getOrCreate()

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("com").setLevel(Level.OFF)
    eco_distribution_train()
    spark.stop()
  }

  def eco_distribution_train() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val players = rdd
      .filter(x => x.getInteger("human_players") == 10)
      // get all the players and the win relationship
      .map(x => (x.get("players").asInstanceOf[ArrayList[org.bson.Document]].toSeq, x.getBoolean("radiant_win")))
      // split the players from two teams
      .map(x => (x._1.map(y => y.getInteger("gold_per_min")).splitAt(5), x._2))
      // map relation ship to a value, 1 for win, 0 for lose
      .map(x => ((x._1._1, if(x._2) 1 else 0), ((x._1._2, if(x._2) 0 else 1))))
      // flatten the tuple, each game gives two data points, one for each team
      .flatMap(x => Seq(x._1, x._2))
      .map(x => ((x._1, x._1.reduce((a, b) => a + b)), x._2))
      // compute the percentage of the economy of each player, sort it in ascending order
      .map(x => (x._2, Vectors.dense( (x._1._1.map(a => a.toDouble / x._1._2 )).sortWith(_ < _).toArray)))
      // .take(10)
      // .foreach(println)
      val training_data = spark.createDataFrame(players).toDF("label", "features")
      val lr = new LogisticRegression
      val model = lr.fit(training_data)
      model.write.overwrite().save("./models/eco_distribute_model")
      val summary = model.binarySummary
      val accuracy = summary.accuracy
      println(s"The accuracy is $accuracy")


  }

}