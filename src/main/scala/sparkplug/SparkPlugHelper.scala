package sparkplug

import org.apache.spark.sql.SparkSession
import sparkplug.models.PlugRule

class SparkPlugHelper(val spark: SparkSession) {

  def readPlugRulesFrom(path: String): Array[PlugRule] = {
    import spark.implicits._
    spark.read.json(path).as[PlugRule].collect()
  }

}

object SparkPlugHelper {
  implicit def sparkContext(sparkSession: SparkSession): SparkPlugHelper =
    new SparkPlugHelper(sparkSession)
}
