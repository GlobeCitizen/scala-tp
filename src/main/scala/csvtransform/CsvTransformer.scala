package csvtransform

import org.apache.spark.sql.SparkSession

object CsvTransformer {

  def delete_client(sparkSession: SparkSession, hdfsPath: String, clientId: Long): Unit = {
    val dataset = CsvReader.readCsv(sparkSession, hdfsPath)
    // get all row except the row with clientId
    val filteredDataset = dataset.filter(row => row.clientId != clientId)
    //overwrite the csv file with the filtered dataset
    CsvWriter.writeCsv(filteredDataset, hdfsPath)
  }

}
