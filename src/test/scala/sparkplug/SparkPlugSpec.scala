package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest._
import sparkplug.models.{PlugAction, PlugRule, PlugRuleValidationError}

class SparkPlugSpec extends FlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession.builder
    .config(new SparkConf())
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  "SparkPlug" should "return input df as is" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.create()
    sparkPlug.plug(df, List.empty).right.get should be (df)
  }

  it should "add plug details to the df if enabled" in {
    val df = spark.emptyDataFrame
    val sparkPlug = SparkPlug.builder.enablePlugDetails.create()
    sparkPlug.plug(df, List.empty).right.get.schema.fieldNames should contain("plugDetails")
  }

  case class TestRow(title: String, brand: String, price: Int)
  it should "validate rules if enabled" in {
    val df = spark.createDataFrame(List(
      TestRow("iPhone", "Apple", 300),
      TestRow("Galaxy", "Samsung", 200)
    ))
    val sparkPlug = SparkPlug.builder.enableRulesValidation.create()
    val invalidRules = List(
      PlugRule("rule1", "title like '%iPhone%'", Seq(PlugAction("randomField", "1"))),
      PlugRule("rule2", "title like '%iPhone%'", Seq(PlugAction("price", "too high")))
    )
    sparkPlug.plug(df, invalidRules).left.get should be (List(
      PlugRuleValidationError("rule1", "Field randomField not found in the schema."),
      PlugRuleValidationError("rule2", "Value \"too high\" cannot be assigned to field price.")
    ))
  }
}
