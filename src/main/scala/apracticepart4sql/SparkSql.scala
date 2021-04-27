package apracticepart4sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SparkSql extends App {


  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("SparkSql")
    .config("spark.master", "local")
    // .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // regular dataframe api
  carsDF.select(col("Name"))
    .where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val USCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin)

  // USCarsDF.show()

  spark.sql(
    """
      |create database rtjvm
      |""".stripMargin
  )
  spark.sql(
    """
      |use rtjvm
      |""".stripMargin
  )
  val databaseDF = spark.sql(
    """
      |show databases
      |""".stripMargin
  )
  // databaseDF.show()

  // transfer tables from a DB to Spark tables

}
