package sparkplug.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.ArrayType
import sparkplug.models.PlugDetail
import sparkplug.utils.ReflectionUtil

abstract class AddPlugDetailUDF
    extends UDF4[Seq[Row], String, String, Seq[String], Seq[Row]] {
  override def call(t1: Seq[Row], t2: String, t3: String, t4: Seq[String]) = {
    addPlugDetails(t1, t2, t3, t4)
  }

  def addPlugDetails(plugDetails: Seq[Row],
                     ruleName: String,
                     ruleVersion: String,
                     fields: Seq[String]): Seq[Row]
}

class DefaultAddPlugDetailUDF extends AddPlugDetailUDF {
  override def addPlugDetails(plugDetails: Seq[Row],
                              ruleName: String,
                              ruleVersion: String,
                              fields: Seq[String]) = {
    plugDetails :+ new GenericRowWithSchema(
      Array(ruleName, ruleVersion, fields),
      SparkPlugUDFs.defaultPlugDetailSchema)
  }
}

object SparkPlugUDFs {
  val defaultPlugDetailSchema =
    ReflectionUtil.caseClassToSparkSchema[PlugDetail]
  val defaultPlugDetailsSchema = ArrayType(defaultPlugDetailSchema)
  val defaultPlugDetailsColumns = "plugDetails"
  val defaultAddPlugDetailUDF = new DefaultAddPlugDetailUDF()
}
