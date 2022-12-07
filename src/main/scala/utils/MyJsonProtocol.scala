package utils

import configs.ColumnTypeConfig
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val columnTypeConfigFormat: RootJsonFormat[ColumnTypeConfig] = jsonFormat2(ColumnTypeConfig)
}
