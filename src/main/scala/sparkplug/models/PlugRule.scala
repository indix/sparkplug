package sparkplug.models

case class PlugAction(key: String, value: String, convertedValue: Option[Any] = None) {
  val updateKey = key.split('.').head
}

object PlugRule {
  val plugDetailsColumn = "plugDetails"
  val plugDetailsColumnUpdated = s"${plugDetailsColumn}_updated"
}