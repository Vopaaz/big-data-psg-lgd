package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.sql.SparkSession

object Hello extends App {
  val spark = SparkSession.builder
    .master("local")
    .appName("Test")
    .getOrCreate()
  println("===========HERE===========")
  val myRange = spark.range(1000).toDF("number")
  myRange.show()
  println("===========STOP===========")

  spark.stop()
}
