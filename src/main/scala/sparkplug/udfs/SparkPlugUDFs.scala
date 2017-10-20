package sparkplug.udfs

import org.apache.spark.sql.SQLContext
import sparkplug.models.PlugDetail

object SparkPlugUDFs {
  private def addPlugDetail(input: Seq[PlugDetail],
                            ruleName: String,
                            fieldNames: Seq[String]) = {
    input :+ PlugDetail(ruleName, fieldNames)
  }
  def registerUDFs(sqlContext: SQLContext) = {
    sqlContext.udf.register("addPlugDetail",
                            (w: Seq[PlugDetail], x: String, y: Seq[String]) =>
                              addPlugDetail(w, x, y))
  }
}
