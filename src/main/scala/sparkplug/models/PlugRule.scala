package sparkplug.models

import org.apache.spark.sql.types._

import scala.util.{Success, Try}

case class PlugRuleValidationError(name: String, error: String)

case class PlugAction(key: String, value: String, convertedValue: Option[Any] = None) {
  val updateKey = key.split('.').head
}

object PlugRule {
  val plugDetailsColumn = "plugDetails"
  val plugDetailsColumnUpdated = s"${plugDetailsColumn}_updated"
}

case class PlugRule(name: String, condition: String, actions: Seq[PlugAction]) {

  lazy val fieldNames = actions.map(x => s""""${x.key}"""").mkString("array(", ",", ")")

  def oldKey(actionKey: String) = s"${actionKey}_${name}_old"
  def newKey(actionKey: String) = s"${actionKey}_new"

  def convertedActions(schema: StructType) = {
    val fields = buildFieldsMap(schema).toMap
    actions
      .map(x => x.copy(
        convertedValue = convertActionValueTo(x.value, fields(x.key)).toOption
      ))
  }

  def convertActionValueTo(actionValue: String, dataType: DataType) = {
    if (actionValue.contains('`')) {
      Success(actionValue.replace("`", ""))
    } else {
      Try(dataType match {
        case IntegerType => actionValue.toInt
        case DoubleType  => s"cast(${actionValue.toDouble} as double)"
        case StringType  => s""""${actionValue.toString}""""
      })
    }
  }

  def validate(schema: StructType): List[PlugRuleValidationError] = {
    val fields = buildFieldsMap(schema).toMap

    actions.flatMap((action: PlugAction) => {
      fields.get(action.key) match {
        case None =>
          Some(validationError(s"Field ${action.key} not found in the schema."))
        case Some(x) if convertActionValueTo(action.value, x).isFailure =>
          Some(validationError(s"""Value "${action.value}" cannot be assigned to field ${action.key}."""))
        case _ =>
          None
      }
    }).toList
  }

  private def validationError(message: String) = PlugRuleValidationError(name, message)

  private def buildFieldsMap(schema: StructType, prefix: String = ""): Seq[(String, DataType)] = {
    schema.fields.flatMap {
      case f if f.dataType.isInstanceOf[StructType] =>
        (s"$prefix${f.name}", f.dataType) +: buildFieldsMap(f.dataType.asInstanceOf[StructType], s"${f.name}.")
      case f => Seq((s"$prefix${f.name}", f.dataType))
    }
  }
}
