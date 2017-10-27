package sparkplug

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import sparkplug.models.{PlugDetail, PlugRule, PlugRuleValidationError}
import sparkplug.udfs.SparkPlugUDFs

import scala.util.Try

case class SparkPlugCheckpointDetails(checkpointDir: String,
                                      rulesPerStage: Int,
                                      numberOfPartitions: Int)

case class SparkPlug(
    private val isPlugDetailsEnabled: Boolean,
    private val plugDetailsColumn: String,
    private val isValidateRulesEnabled: Boolean,
    private val checkpointDetails: Option[SparkPlugCheckpointDetails],
    private val isAccumulatorsEnabled: Boolean)(implicit val spark: SparkSession) {

  private val tableName = "__plug_table__"

  registerUdf(spark)
  setupCheckpointing(spark, checkpointDetails)

  def plug(in: DataFrame, rules: List[PlugRule])
    : Either[List[PlugRuleValidationError], DataFrame] = {

    val validationResult = Option(isValidateRulesEnabled)
      .filter(identity)
      .map(_ => validate(in.schema, rules))
      .filter(_.nonEmpty)
    if (validationResult.nonEmpty) {
      Left(validationResult.get)
    } else {
      Right(plugDf(in, rules))
    }
  }

  private def plugDf(in: DataFrame, rules: List[PlugRule]) = {
    val out = spark.sparkContext
      .broadcast(rules)
      .value
      .zipWithIndex
      .foldLeft(preProcessInput(in)) {
        case (df: DataFrame, (rule: PlugRule, ruleNumber: Int)) =>
          repartitionAndCheckpoint(applyRule(df, rule), ruleNumber)
      }

    Option(isAccumulatorsEnabled)
      .filter(identity)
      .foreach(_ => {
        val accumulatorChanged =
          spark.sparkContext.longAccumulator(s"SparkPlug.Changed")
        out
          .filter(
            _.getAs[Seq[GenericRowWithSchema]](plugDetailsColumn).nonEmpty)
          .foreach((_: Row) => {
            accumulatorChanged.add(1)
          })
      })

    out
  }

  def validate(schema: StructType, rules: List[PlugRule]) = {
    rules
      .groupBy(_.name)
      .filter(_._2.size > 1)
      .keysIterator
      .map(
        r =>
          PlugRuleValidationError(
            r,
            "Only one version per rule should be applied."))
      .toList ++
      Option(rules.flatMap(_.validate(schema)))
        .filter(_.nonEmpty)
        .getOrElse(rules.flatMap(r => validateRuleSql(schema, r)))
  }

  private def validateRuleSql(schema: StructType, rule: PlugRule) = {
    Try(applyRule(emptyDf(schema), rule)).failed
      .map { t =>
        List(
          PlugRuleValidationError(rule.name, s"[SQL Error] ${t.getMessage}"))
      }
      .getOrElse(List())
  }

  private def emptyDf(schema: StructType) = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
  }

  private def preProcessInput(in: DataFrame) = {
    if (isPlugDetailsEnabled && !in.schema.fields.exists(
          _.name == plugDetailsColumn)) {
      val emptyOverrideDetails = udf(() => Seq[PlugDetail]())
      in.withColumn(plugDetailsColumn, emptyOverrideDetails())
    } else {
      in
    }
  }

  private def registerUdf(spark: SparkSession) = {
    if (isPlugDetailsEnabled) {
      SparkPlugUDFs.registerUDFs(spark.sqlContext)
    }
  }

  private def applyRule(frame: DataFrame, rule: PlugRule) = {
    val output = applySql(
      frame,
      s"select *,${rule.asSql(frame.schema, isPlugDetailsEnabled, plugDetailsColumn)} from $tableName")

    rule.withColumnsRenamed(output, isPlugDetailsEnabled, plugDetailsColumn)
  }

  private def applySql(in: DataFrame, sql: String): DataFrame = {
    in.createOrReplaceTempView(tableName)
    in.sqlContext.sql(sql)
  }

  private def repartitionAndCheckpoint(in: Dataset[Row], ruleNumber: Int) = {
    checkpointDetails.fold(in) { cd =>
      (repartition(cd, ruleNumber) _ andThen checkpoint(cd, ruleNumber))(in)
    }
  }

  private def checkpoint(checkpointDetails: SparkPlugCheckpointDetails,
                         ruleNumber: Int)(in: Dataset[Row]) = {
    if ((ruleNumber + 1) % (2 * checkpointDetails.rulesPerStage) == 0)
      in.checkpoint()
    else in
  }

  private def repartition(checkpointDetails: SparkPlugCheckpointDetails,
                          ruleNumber: Int)(in: Dataset[Row]) = {
    if ((ruleNumber + 1) % checkpointDetails.rulesPerStage == 0)
      in.repartition(checkpointDetails.numberOfPartitions)
    else in
  }

  private def setupCheckpointing(
      spark: SparkSession,
      checkpointDetails: Option[SparkPlugCheckpointDetails]) = {
    checkpointDetails.foreach(cd =>
      spark.sparkContext.setCheckpointDir(cd.checkpointDir))
  }

}

case class SparkPlugBuilder(
    isPlugDetailsEnabled: Boolean = false,
    plugDetailsColumn: String = "plugDetails",
    isValidateRulesEnabled: Boolean = false,
    checkpointDetails: Option[SparkPlugCheckpointDetails] = None,
    isAccumulatorsEnabled: Boolean = false)(implicit val spark: SparkSession) {
  def enablePlugDetails(plugDetailsColumn: String = plugDetailsColumn) =
    copy(isPlugDetailsEnabled = true, plugDetailsColumn = plugDetailsColumn)
  def enableRulesValidation = copy(isValidateRulesEnabled = true)
  def enableCheckpointing(checkpointDir: String,
                          rulesPerStage: Int,
                          numberOfPartitions: Int) =
    copy(
      checkpointDetails = Some(
        SparkPlugCheckpointDetails(checkpointDir,
                                   rulesPerStage,
                                   numberOfPartitions)))

  def enableAccumulators =
    copy(isAccumulatorsEnabled = true, isPlugDetailsEnabled = true)

  def create() =
    new SparkPlug(isPlugDetailsEnabled,
                  plugDetailsColumn,
                  isValidateRulesEnabled,
                  checkpointDetails,
                  isAccumulatorsEnabled)
}

object SparkPlug {
  def builder(implicit spark: SparkSession) = SparkPlugBuilder()
}
