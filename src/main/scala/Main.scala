object Main extends App{
  case class Config(identifiantClient: Long = 0)

  val parser = new scopt.OptionParser[Config]("scala-tp") {
    head("scala-tp", "0.1")
    opt[Long]('i', "identifiant client")
      .action((x, c) => c.copy(identifiantClient = x))
      .required()
      .text("Identifiant client")
  }


  parser.parse(args, Config()) match {
    case Some(config) =>
      execute(config.identifiantClient)
    case None =>
      println("Error")
  }

  def execute(identifiantClient: Long) = {
    val spark = SparkSession.builder()
      .appName("scala-tp")
      .master("local[*]")
      .getOrCreate()
    val csvReader = new CsvReader()
    val csvPath = "src/main/resources/clients.csv"
    val schemaPath = "src/main/resources/clients_schema.json"
    val df = csvReader.readCsvWithSchema(spark, csvPath, schemaPath)
    df.show()
    df.printSchema()
    df.createOrReplaceTempView("clients")
    val result = spark.sql(s"SELECT * FROM clients WHERE identifiant_client = $identifiantClient")
    result.show()
  }

}

