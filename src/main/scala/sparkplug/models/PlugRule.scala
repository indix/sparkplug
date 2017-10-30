package sparkplug.models

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.{Success, Try}

case class PlugAction(key: String, value: String) {
  val updateKey = key.split('.').head
}

case class PlugActionConverted(key: String, value: Any)

case class PlugRule(name: String,
                    version: String,
                    condition: String,
                    actions: Seq[PlugAction]) {

  lazy val fieldNames =
    actions.map(x => s""""${x.key}"""").mkString("array(", ",", ")")

  private val defaultValidation
    : ((Seq[PlugAction], StructType)) => List[PlugRuleValidationError] = _ =>
    List.empty

  private val actionsNotEmpty
    : PartialFunction[(Seq[PlugAction], StructType),
                      List[PlugRuleValidationError]] = {
    case (as, _) if as.isEmpty =>
      List(
        validationError("At the least one action must be specified per rule."))
  }

  private val actionFieldsPresentInSchema
    : PartialFunction[(Seq[PlugAction], StructType),
                      List[PlugRuleValidationError]] = {
    case (as, schema) =>
      val fields = buildFieldsMap(schema).toMap

      as.flatMap((action: PlugAction) => {
          fields.get(action.key) match {
            case None =>
              Some(
                validationError(
                  s"""Field "${action.key}" not found in the schema."""))
            case Some(x) if convertActionValueTo(action.value, x).isFailure =>
              Some(validationError(
                s"""Value "${action.value}" cannot be assigned to field ${action.key}."""))
            case _ =>
              None
          }
        })
        .toList
  }

  def validate(schema: StructType): List[PlugRuleValidationError] = {
    List(actionsNotEmpty, actionFieldsPresentInSchema).flatMap(
      _.applyOrElse((actions, schema), defaultValidation))
  }

  def asSql(schema: StructType, plugDetailsColumn: Option[String]) = {
    val notEqualsBuilder = new ListBuffer[String]
    val builder = new StringBuilder

    val convertedActions = convertActions(schema)
    convertedActions.zipWithIndex.foldLeft(builder) {
      case (b, (action, i)) =>
        val actionKey = action.key
        val actionValue = action.value
        notEqualsBuilder.append(s"not($actionKey <=> $actionValue)")
        val builderWithField = updateField(schema, b, actionKey, actionValue)
        if (i < convertedActions.length - 1) {
          builderWithField.append(",")
        } else {
          builderWithField
        }
    }

    if (plugDetailsColumn.nonEmpty) {
      val notEqualCondition = s"(${notEqualsBuilder.mkString(" or ")})"
      builder.append(
        s""", if($condition and $notEqualCondition, addPlugDetail(${plugDetailsColumn.get}, "$name", "$version", $fieldNames), ${plugDetailsColumn.get}) as ${updatedPlugDetailsColumn(
          plugDetailsColumn.get)}""")
    }

    builder.mkString
  }

  def withColumnsRenamed(dataset: Dataset[Row],
                         plugDetailsColumn: Option[String],
                         isKeepOldField: Boolean) = {
    val pluggedDf = actions
      .foldLeft(dataset)((overridden, action) => {
        val modified = overridden
          .withColumnRenamed(action.updateKey, oldKey(action.updateKey))
          .withColumnRenamed(newKey(action.updateKey), action.updateKey)

        if (!isKeepOldField)
          modified.drop(oldKey(action.key))
        else
          modified
      })

    plugDetailsColumn.fold(pluggedDf) { c =>
      pluggedDf
        .drop(c)
        .withColumnRenamed(updatedPlugDetailsColumn(c), c)
    }
  }

  private def updatedPlugDetailsColumn(plugDetailsColumn: String) =
    s"${plugDetailsColumn}_updated"

  private def updateField(schema: StructType,
                          builder: mutable.StringBuilder,
                          actionKey: String,
                          actionValue: Any) = {
    actionKey match {
      case x if x.contains('.') =>
        val Array(parent, child) = x.split('.')
        val parentSchema = schema
          .filter((field: StructField) => field.name == parent)
          .head
          .dataType
          .asInstanceOf[StructType]
        builder.append(s"if($parent is null, null, named_struct(")
        val namedStruct = parentSchema.fields
          .map((field: StructField) => {
            if (field.name == child)
              s"'${field.name}', if($condition, $actionValue, $actionKey)"
            else
              s"'${field.name}',$parent.${field.name}"
          })
          .mkString(",")
        builder.append(namedStruct)
        builder.append(s")) as ${newKey(parent)}")
      case _ =>
        builder.append(
          s"if($condition, $actionValue, $actionKey) as ${newKey(actionKey)}")
    }
  }

  private def convertActions(schema: StructType) = {
    val fields = buildFieldsMap(schema).toMap
    actions
      .map(
        x =>
          PlugActionConverted(
            x.key,
            convertActionValueTo(x.value, fields(x.key)).getOrElse(null)))
  }

  private def convertActionValueTo(actionValue: String, dataType: DataType) = {
    if (actionValue.contains('`')) {
      Success(actionValue.replace("`", ""))
    } else {
      Try(dataType match {
        case IntegerType => actionValue.toInt
        case DoubleType => s"cast(${actionValue.toDouble} as double)"
        case StringType => s""""${actionValue.toString}""""
      })
    }
  }

  private def validationError(message: String) =
    PlugRuleValidationError(name, message)

  private def buildFieldsMap(schema: StructType,
                             prefix: String = ""): Seq[(String, DataType)] = {
    schema.fields.flatMap {
      case f if f.dataType.isInstanceOf[StructType] =>
        (s"$prefix${f.name}", f.dataType) +: buildFieldsMap(
          f.dataType.asInstanceOf[StructType],
          s"$prefix${f.name}.")
      case f => Seq((s"$prefix${f.name}", f.dataType))
    }
  }

  private def oldKey(actionKey: String) = s"${actionKey}_${name}_old"

  private def newKey(actionKey: String) = s"${actionKey}_new"
}
