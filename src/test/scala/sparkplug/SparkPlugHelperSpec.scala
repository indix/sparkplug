package sparkplug

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}
import sparkplug.models.{PlugAction, PlugRule}

class SparkPlugHelperSpec extends FlatSpec with Matchers {
  implicit val spark: SparkSession = SparkSession.builder
    .config(new SparkConf())
    .enableHiveSupport()
    .master("local[*]")
    .getOrCreate()

  import SparkPlugHelper._
  "SparkPlugHelper" should "read rule from json" in {
    val path = getClass.getClassLoader.getResource("rules.json").getPath
    val rules = spark.readPlugRulesFrom(path)

    rules.length should be(2)
    rules(0) should be(
      PlugRule("rule1",
               "title like '%iPhone%'",
               Seq(PlugAction("title", "Apple iPhone"))))
    rules(1) should be(
      PlugRule("rule2",
               "title like '%Galaxy%'",
               Seq(PlugAction("title", "Samsung Galaxy"))))
  }
}
