package csvtransform

import models.Client
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{Dataset, SaveMode}
import scala.sys.process._

object CsvWriter {
  def writeCsv(filteredDataset: Dataset[Client], path: String, fileName: String): Unit = {

    // overwrite the csv file with the filtered dataset
    filteredDataset.checkpoint(true)
      .coalesce(1)
      .write
      .format("csv")
      .option("header", "true")
      .mode(SaveMode.Overwrite)
      .save(new Path(path, fileName).toString)

    // delete the tmp folder
    val hadoopConf = new Configuration()
    val hdfs = FileSystem.get(hadoopConf)
    val tmpPath = new Path("tmp")
    hdfs.delete(tmpPath, true)
  }
}
