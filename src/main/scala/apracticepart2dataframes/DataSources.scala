package apracticepart2dataframes

import apracticepart2dataframes.DataFrameBasic.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
    * Reading a dataframe
    * - format
    * - schema (Optional) || infer
    * - 0 or 1+ option
    */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce schema
    .option("mode", "failFast") //
    .option("path", "src/main/resources/data/cars.json") //
    .load()

  // carsDF.show()

  // alternative
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    )).load()

  // write DF ** require /format, /save mode = overwrite, append, ignore, errorIfExists
  // path

  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dup.json")
    .save()

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip gzip
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stockSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType),
  ))

  spark.read
    .schema(stockSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true")
    .option("sep", ",")
    .option("NullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text
  spark.read
    .text("src/main/resources/data/sampleTextFile.txt").show()

  // reading from db
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url","jdbc:postgresql://128.199.85.229:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()

  employeesDF.show()

  /*
  * Exercise
  *
  * */

  val moviesDF = spark.read.json("src/main/resources/data/movies.json")
  // CSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/moviesNew.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/moviesNew.parquet")

  // save to db
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url","jdbc:postgresql://128.199.85.229:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.movies")
    .save()
































}
