package sparkplug.udfs

import org.apache.spark.sql.Row
import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType
import sparkplug.models.PlugDetail
import sparkplug.utils.ReflectionUtil

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

abstract class AddPlugDetailUDF[+T <: Product: TypeTag: ClassTag]
    extends UDF4[Seq[Row], String, String, Seq[String], Seq[Row]] {
  override def call(t1: Seq[Row], t2: String, t3: String, t4: Seq[String]) =
    addPlugDetails(t1, t2, t3, t4)

  lazy val plugDetailSchema  = ReflectionUtil.caseClassToSparkSchema[T]
  lazy val plugDetailsSchema = ArrayType(plugDetailSchema)

  def emptyPlugDetails = udf(() => Seq[T]())

  def addPlugDetails(plugDetails: Seq[Row], ruleName: String, ruleVersion: String, fields: Seq[String]): Seq[Row]
}

class DefaultAddPlugDetailUDF extends AddPlugDetailUDF[PlugDetail] {

  override def addPlugDetails(plugDetails: Seq[Row], ruleName: String, ruleVersion: String, fields: Seq[String]) =
    plugDetails :+ new GenericRowWithSchema(Array(ruleName, ruleVersion, fields), plugDetailSchema)
}

object SparkPlugUDFs {
  val defaultPlugDetailsColumn = "plugDetails"
  val defaultAddPlugDetailUDF  = new DefaultAddPlugDetailUDF()
}
