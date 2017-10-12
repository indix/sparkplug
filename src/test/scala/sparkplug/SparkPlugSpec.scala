package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._

class SparkPlugSpec extends FlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession.builder
    .config(new SparkConf())
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  "SparkPlug" should "return input df as is" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.create()
    sparkPlug.plug(df) should be (df)
  }

  it should "add plug details to the df if enabled" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.enablePlugDetails.create()
    sparkPlug.plug(df).schema.fieldNames should contain("plugDetails")
  }
}
