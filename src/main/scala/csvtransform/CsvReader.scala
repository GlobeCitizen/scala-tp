package csvtransform

import configs.ColumnTypeConfig
import models.Client
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import spray.json._
import utils.MyJsonProtocol._

object CsvReader {

  def readJsonSchema(sparkSession: SparkSession, path: String): StructType = {
    val source = scala.io.Source.fromFile(path)
    val lines = try source.mkString finally source.close()
    val json = lines.parseJson
    // get columns field from json
    val columns = json.asJsObject.fields("columns").asInstanceOf[JsArray]
    // convert json to case class
    val columnTypeConfigs = columns.elements.map(_.convertTo[ColumnTypeConfig])

    // create schema as StructType from columnTypeConfigs
    val schema = StructType(columnTypeConfigs.map(columnTypeConfig => {
      val dataType = columnTypeConfig.dataType match {
        case "string" => StringType
        case "integer" => IntegerType
        case "double" => DoubleType
        case "long" => LongType
        case "datetime" => TimestampType
        case _ => StringType
      }
      StructField(columnTypeConfig.name, dataType, nullable = false)
    }))
    schema
  }

  def readCsv(sparkSession: SparkSession, path: String): Dataset[Client] = {
    val schema = readJsonSchema(sparkSession, "src/main/resources/client_schema.json")

    val df = sparkSession.read
      .format("csv")
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .load(path)

    val encoder = org.apache.spark.sql.Encoders.product[Client]

    val dataset = df.as(encoder)
    dataset
  }

}
