class CsvReader {
  // read csv to dataframe with a schema from a json file
  def readCsvWithSchema(spark: SparkSession, csvPath: String, schemaPath: String): DataFrame = {
    val schema = spark.read.json(schemaPath).schema
    spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .schema(schema)
      .csv(csvPath)
  }

}
