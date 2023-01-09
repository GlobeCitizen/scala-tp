package csvtransform

import models.Client
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, hash}

object CsvTransformer {
  def delete_client(sparkSession: SparkSession, hdfsPath: String, fileName: String, clientId: Long): Unit = {
    val dataset = CsvReader.readCsv(sparkSession, hdfsPath, fileName)
    // get all row except the row with clientId
    val filteredDataset = dataset.filter(row => row.clientId != clientId)
    //overwrite the csv file with the filtered dataset
    CsvWriter.writeCsv(filteredDataset, hdfsPath, fileName)
  }

  // hash firstname and last name and address of a client in dataset
  def hash_client(sparkSession: SparkSession, hdfsPath: String, fileName: String, clientId: Long): Unit = {
    val dataset = CsvReader.readCsv(sparkSession, hdfsPath, fileName)
    // hash firstname and last name and address of the client matching clientID using withColumn
    import sparkSession.implicits._
    val client = dataset.filter(row => row.clientId == clientId)
      .withColumn("firstName", hash(col("firstName")))
      .withColumn("lastName", hash(col("lastName")))
      .withColumn("address", hash(col("address"))).as[Client]

    // insert client into the dataset
    val filteredDataset = dataset.filter(row => row.clientId != clientId).union(client)

    //overwrite the csv file with the filtered dataset
    CsvWriter.writeCsv(filteredDataset, hdfsPath, fileName)
  }


}