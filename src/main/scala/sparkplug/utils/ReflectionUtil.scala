package sparkplug.utils

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object ReflectionUtil {
  def extractFieldNames[T <: Product](implicit m: Manifest[T]) =
    m.runtimeClass.getDeclaredFields.map(_.getName)

  def caseClassToSparkSchema[T <: Product](implicit m: Manifest[T]) = {
    ScalaReflection.schemaFor[T].dataType.asInstanceOf[StructType]
  }
}
