package sparkplug.models

import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
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

  def asSql(schema: StructType, addPlugDetails: Boolean = false) = {
    val notEqualsBuilder = new ListBuffer[String]
    val builder = new StringBuilder
    convertedActions(schema).foldLeft(builder)((builder, action) => {
      val actionKey = action.key
      val actionValue = action.convertedValue.getOrElse(action.value)
      notEqualsBuilder.append(s"not($actionKey <=> $actionValue)")
      updateField(schema, builder, actionKey, actionValue)
    })

    if(addPlugDetails) {
      val notEqualCondition = s"(${notEqualsBuilder.mkString(" or ")})"
      builder.append(s""", if($condition and $notEqualCondition, addPlugDetail(${PlugRule.plugDetailsColumn}, "$name", $fieldNames), ${PlugRule.plugDetailsColumn}) as ${PlugRule.plugDetailsColumnUpdated}""")
    }

    builder.mkString
  }

  def withColumnsRenamed(dataset: Dataset[Row]) = {
    actions.foldLeft(dataset)((overridden, action) => {
      val modified = overridden
        .withColumnRenamed(action.updateKey, oldKey(action.updateKey))
        .withColumnRenamed(newKey(action.updateKey), action.updateKey)

      modified.drop(oldKey(action.key))
    }).drop(PlugRule.plugDetailsColumn)
      .withColumnRenamed(PlugRule.plugDetailsColumnUpdated, PlugRule.plugDetailsColumn)
  }

  def isRuleAppliedOn(row: Row) = {
    row.getAs[Seq[GenericRowWithSchema]](PlugRule.plugDetailsColumn).exists(_.getAs[String]("ruleId") == name)
  }

  private def updateField(schema: StructType, builder: mutable.StringBuilder, actionKey: String, actionValue: Any) = {
    actionKey match {
      case x if x.contains('.') =>
        val Array(parent, child) = x.split('.')
        val parentSchema = schema.filter((field: StructField) => field.name == parent).head.dataType.asInstanceOf[StructType]
        builder.append(s"if($parent is null, null, named_struct(")
        val namedStruct = parentSchema.fields.map((field: StructField) => {
          if (field.name == child)
            s"'${field.name}', if($condition, $actionValue, $actionKey)"
          else
            s"'${field.name}',$parent.${field.name}"
        }).mkString(",")
        builder.append(namedStruct)
        builder.append(s")) as ${newKey(parent)}")
      case _ =>
        builder.append(s"if($condition, $actionValue, $actionKey) as ${newKey(actionKey)}")
    }
  }

  private def convertedActions(schema: StructType) = {
    val fields = buildFieldsMap(schema).toMap
    actions
      .map(x => x.copy(
        convertedValue = convertActionValueTo(x.value, fields(x.key)).toOption
      ))
  }

  private def convertActionValueTo(actionValue: String, dataType: DataType) = {
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

  private def validationError(message: String) = PlugRuleValidationError(name, message)

  private def buildFieldsMap(schema: StructType, prefix: String = ""): Seq[(String, DataType)] = {
    schema.fields.flatMap {
      case f if f.dataType.isInstanceOf[StructType] =>
        (s"$prefix${f.name}", f.dataType) +: buildFieldsMap(f.dataType.asInstanceOf[StructType], s"${f.name}.")
      case f => Seq((s"$prefix${f.name}", f.dataType))
    }
  }

  private def oldKey(actionKey: String) = s"${actionKey}_${name}_old"

  private def newKey(actionKey: String) = s"${actionKey}_new"
}
