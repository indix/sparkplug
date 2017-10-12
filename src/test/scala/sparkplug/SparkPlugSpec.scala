package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkPlugSpec extends FlatSpec with Matchers {
  "SparkPlug" should "return input df as is" in {
    implicit val spark: SparkSession = SparkSession.builder
      .config(new SparkConf())
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.enablePlugDetails.create()
    sparkPlug.plug(df) should be (df)
  }
}
