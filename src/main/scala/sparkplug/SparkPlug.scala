package sparkplug

import org.apache.spark.sql.{DataFrame, SparkSession}

case class SparkPlug(isPlugDetailsEnabled: Boolean)(implicit val spark : SparkSession) {
  def plug(in: DataFrame) = {
    in
  }
}

case class SparkPlugBuilder(isPlugDetailsEnabled: Boolean = false)(implicit val spark : SparkSession) {
  def enablePlugDetails = copy(isPlugDetailsEnabled = false)

  def create() = new SparkPlug(isPlugDetailsEnabled)
}

object SparkPlug {
  def builder(implicit spark : SparkSession) = SparkPlugBuilder()
}
