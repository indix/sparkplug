package sparkplug

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkplug.models.{PlugDetail, PlugRule, PlugRuleValidationError}
import sparkplug.udfs.SparkPlugUDFs

case class SparkPlug(isPlugDetailsEnabled: Boolean, isValidateRulesEnabled: Boolean)(implicit val spark : SparkSession) {
  def plug(in: DataFrame, rules: List[PlugRule]): Either[List[PlugRuleValidationError], DataFrame] = {
    val validationResult = validateRules(in, rules)
    if(validationResult.nonEmpty) {
      Left(validationResult)
    } else {
      registerUdf(spark)
      Right(preProcessInput(in))
    }
  }

  private def validateRules(in: DataFrame, rules: List[PlugRule]) = {
    if(isValidateRulesEnabled)
      rules.flatMap(_.validate(in.schema))
    else
      List.empty
  }

  private def preProcessInput(in: DataFrame) = {
    if (isPlugDetailsEnabled && !in.schema.fields.exists(_.name == PlugRule.plugDetailsColumn)) {
      val emptyOverrideDetails = udf(() => Seq[PlugDetail]())
      in.withColumn(PlugRule.plugDetailsColumn, emptyOverrideDetails())
    } else {
      in
    }
  }

  private def registerUdf(spark: SparkSession) = {
    if(isPlugDetailsEnabled) {
      SparkPlugUDFs.registerUDFs(spark.sqlContext)
    }
  }

}

case class SparkPlugBuilder(isPlugDetailsEnabled: Boolean = false, isValidateRulesEnabled: Boolean = false)(implicit val spark : SparkSession) {
  def enablePlugDetails = copy(isPlugDetailsEnabled = true)
  def enableRulesValidation = copy(isValidateRulesEnabled = true)

  def create() = new SparkPlug(isPlugDetailsEnabled, isValidateRulesEnabled)
}

object SparkPlug {
  def builder(implicit spark : SparkSession) = SparkPlugBuilder()
}
