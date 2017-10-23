package sparkplug.models

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.scalatest.{FlatSpec, Matchers}

class PlugRuleSpec extends FlatSpec with Matchers {
  "PlugRule#validate" should "validate empty actions" in {
    val rule = PlugRule("rule1", "version1", "condition", Seq())

    val errors = rule.validate(StructType(List()))
    errors.length should be (1)
    errors.head.name should be ("rule1")
    errors.head.error should be ("At the least one action must be specified per rule.")
  }

  it should "error if field not in schema" in {
    val rule = PlugRule("rule1", "version1", "condition",Seq(PlugAction("title",  "title 1")))

    val errors = rule.validate(StructType(List()))
    errors.length should be (1)
    errors.head.name should be ("rule1")
    errors.head.error should be ("Field \"title\" not found in the schema.")
  }

  it should "ensure action fields present in schema" in {
    val rule1 = PlugRule("rule1", "version1", "condition",Seq(PlugAction("title", "title 1")))

    val errors1 = rule1.validate(StructType(List(StructField("title", StringType))))
    errors1.length should be (0)

    val rule2 = PlugRule("rule2", "version1", "condition", Seq(PlugAction("title.main", "title 1")))

    val errors2 = rule2.validate(StructType(List(StructField("title", StructType(List(StructField("main", StringType)))))))
    errors2.length should be (0)

    val rule3 = PlugRule("rule3", "version1", "condition",Seq(PlugAction("title.main.first", "title 1")))

    val errors3 = rule3.validate(StructType(List(
      StructField("title", StructType(List(
        StructField("main", StructType(List(
          StructField("first", StringType)
        )))))))))
    errors3.length should be (0)

    val rule4 = PlugRule("rule3", "version1", "condition", Seq(PlugAction("title", "`null`")))
    val errors4 = rule4.validate(StructType(List(
      StructField("title", StructType(List(
        StructField("main", StructType(List(
          StructField("first", IntegerType)
        )))))))))
    errors4.length should be (0)
  }

  it should "error if incompatible value set" in {
    val rule1 = PlugRule("rule1", "version1", "condition", Seq(PlugAction("title", "title 1")))

    val errors1 = rule1.validate(StructType(List(StructField("title", IntegerType))))
    errors1.length should be (1)
    errors1.head.error should be ("Value \"title 1\" cannot be assigned to field title.")

    val rule2 = PlugRule("rule2", "version1", "condition", Seq(PlugAction("title.main", "title 1")))

    val errors2 = rule2.validate(StructType(List(StructField("title", StructType(List(StructField("main", IntegerType)))))))
    errors2.length should be (1)
    errors2.head.error should be ("Value \"title 1\" cannot be assigned to field title.main.")

    val rule3 = PlugRule("rule3", "version1", "condition", Seq(PlugAction("title.main.first", "title 1")))

    val errors3 = rule3.validate(StructType(List(
      StructField("title", StructType(List(
        StructField("main", StructType(List(
          StructField("first", IntegerType)
        )))))))))
    errors3.length should be (1)
    errors3.head.error should be ("Value \"title 1\" cannot be assigned to field title.main.first.")

    val rule4 = PlugRule("rule3", "version1", "condition", Seq(PlugAction("title", "title 1")))

    val errors4 = rule4.validate(StructType(List(
      StructField("title", StructType(List(
        StructField("main", StructType(List(
          StructField("first", IntegerType)
        )))))))))
    errors4.length should be (1)
    errors4.head.error should be ("Value \"title 1\" cannot be assigned to field title.")
  }
}
