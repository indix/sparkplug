package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkPlugSpec extends FlatSpec with Matchers {
  "SparkPlug" should "return input df as is" in {
    val spark = SparkSession.builder
      .config(new SparkConf())
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val df = spark.emptyDataFrame
    new SparkPlug().plug(df) should be (df)
  }
}
