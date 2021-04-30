package utils

import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame

object DataFrameUtil {

  def saveCsvAsSingFile(
                       df: DataFrame,             // must be small
                       format: String = "csv",    // csv, parquet
                       sc: SparkContext,          // pass in spark.sparkContext
                       tmpFolder: String,         // will be deleted, so make sure it doesn't already exist
                       filename: String,          // the full filename you want outputted
                       saveMode: String = "error", // Spark default is error, overwrite and append are also common
                       options: Map[String, String]
                     ): Unit = {
    df.repartition(1)
      .write
      .mode(saveMode)
      .format(format)
      .options(options)
      .save(tmpFolder)
    val conf    = sc.hadoopConfiguration
    val src     = new Path(tmpFolder)
    val fs      = src.getFileSystem(conf)
    val oneFile = fs.listStatus(src).map(x => x.getPath.toString()).find(x => x.endsWith(format))
    val srcFile = new Path(oneFile.getOrElse(""))
    val dest    = new Path(filename)
    fs.rename(srcFile, dest)
  }
}
