import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.SparkConf
import scala.collection.JavaConversions._
import org.apache.log4j.Logger
import org.apache.log4j.Level

object FirstBloodPredict {

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
    first_blood_predict()
    spark.stop()
  }

  def first_blood_predict() {
    val rdd = MongoSpark.load(spark.sparkContext)
    val fb_and_dur = rdd.map(x => (x.getInteger("duration").toDouble, Vectors.dense(x.getInteger("first_blood_time").toDouble)))
    val training_data = spark.createDataFrame(fb_and_dur).toDF("label", "features").limit(1000)
    val model = LinearRegressionModel.load("./models/first_blood_model")
    val predictions = model.transform(training_data)
    val regEval = new RegressionEvaluator().setMetricName("rmse")
    val rmse = regEval.evaluate(predictions)
    println(s"Root Mean Squared Error on test data: $rmse")
  }

}