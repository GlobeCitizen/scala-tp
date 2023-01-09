import configs.Config
import csvtransform.{CsvReader, CsvTransformer}
import org.apache.spark.sql.SparkSession
import scopt.OParser

object Main extends App{
  // initiate spark session
  val sparkSession = SparkSession.builder()
    .appName("Spark CSV Reader")
    .master("local[*]")
    .getOrCreate()

  sparkSession.sparkContext.setCheckpointDir("tmp")
  sparkSession.sparkContext.setLogLevel("ERROR")

  val builder = OParser.builder[Config]
  val parser = {
    import builder._
    OParser.sequence(
      programName("myApp"),
      head("myApp", "1.0"),
      opt[String]('i', "clientId")
        .action((x, c) => c.copy(clientId = x.toLong))
        .text("clientId is a long property")
        .required(),
      opt[String]('a', "action")
        .action((x, c) => c.copy(action = x))
        .text("action is a string property")
        .required(),
      opt[String]('p', "hdfsPath")
        .action((x, c) => c.copy(hdfsPath = x))
        .text("hdfsPath is a string property")
        .required(),
      opt[String]('f', "fileName")
        .action((x, c) => c.copy(fileName = x))
        .text("fileName is a string property")
        .required()
    )
  }

  OParser.parse(parser, args, Config()) match {
    case Some(config) => {
      config.action match {
        case "delete" => {
          CsvTransformer.delete_client(sparkSession, config.hdfsPath, config.fileName, config.clientId)
          println(s"Client ${config.clientId} deleted")
        }
        case "hash" => {
          CsvTransformer.hash_client(sparkSession, config.hdfsPath, config.fileName, config.clientId)
          println(s"Client ${config.clientId} hashed")
        }
        case _ => println("Action not found")
      }
    }
    case _ => println("Config is invalid")
  }
}

