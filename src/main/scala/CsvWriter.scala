class CsvWriter {
  //write the data to a existant csv file
  def writeCsv(df: DataFrame, csvPath: String): Unit = {
    df.write
      .option("header", "true")
      .option("delimiter", ",")
      .mode("append")
      .csv(csvPath)
  }
}
