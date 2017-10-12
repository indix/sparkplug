package sparkplug

import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}
import sparkplug.models.PlugDetail
import sparkplug.udfs.SparkPlugUDFs

case class SparkPlug(isPlugDetailsEnabled: Boolean)(implicit val spark : SparkSession) {

  def plug(in: DataFrame) = {
    registerUdf(spark)
    preprocessInput(in)
  }

  private def preprocessInput(in: DataFrame) = {
    if (isPlugDetailsEnabled && !in.schema.fields.exists(_.name == "plugDetails")) {
      val emptyOverrideDetails = udf(() => Seq[PlugDetail]())
      in.withColumn("plugDetails", emptyOverrideDetails())
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

case class SparkPlugBuilder(isPlugDetailsEnabled: Boolean = false)(implicit val spark : SparkSession) {
  def enablePlugDetails = copy(isPlugDetailsEnabled = true)

  def create() = new SparkPlug(isPlugDetailsEnabled)
}

object SparkPlug {
  def builder(implicit spark : SparkSession) = SparkPlugBuilder()
}
