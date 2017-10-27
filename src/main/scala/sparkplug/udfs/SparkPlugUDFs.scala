package sparkplug.udfs

import org.apache.spark.sql.api.java.UDF4
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.{Row, SQLContext}
import sparkplug.models.PlugDetail
import sparkplug.utils.ReflectionUtil

class AddPlugDetailUDF
    extends UDF4[Seq[Row], String, String, Seq[String], Seq[Row]] {
  override def call(t1: Seq[Row], t2: String, t3: String, t4: Seq[String]) = {
    t1 :+ new GenericRowWithSchema(Array(t2, t3, t4),
                                   SparkPlugUDFs.defaultPlugDetailSchema)
  }
}

object SparkPlugUDFs {
  val defaultPlugDetailSchema =
    ReflectionUtil.caseClassToSparkSchema[PlugDetail]
  val defaultPlugDetailsSchema = ArrayType(defaultPlugDetailSchema)

  def registerUDFs(sqlContext: SQLContext) = {
    sqlContext.udf.register("addPlugDetail",
                            new AddPlugDetailUDF(),
                            defaultPlugDetailsSchema)
  }
}
