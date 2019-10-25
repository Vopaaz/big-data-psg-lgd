import org.apache.spark.sql.SparkSession
import com.mongodb.spark._
import com.mongodb.spark.sql._

object SimpleApp {

  val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Test")
      .config("spark.master", "local")
      .getOrCreate()

  def main(args: Array[String]) {
    val logFile = "/home/yyc/csci5717/big-data-psg-lgd/src/main/resources/testspark.txt" // Should be some file on your system
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val logData = spark.read.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    spark.stop()
  }

}