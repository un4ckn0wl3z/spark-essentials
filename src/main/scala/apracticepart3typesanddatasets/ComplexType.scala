package apracticepart3typesanddatasets

import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{array_contains, col, current_date, current_timestamp, datediff, expr, size, split, struct, to_date}

object ComplexType extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("Complex Spark Types")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // Dates

  val movieWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy")
    .as("Actual_Release"))

  movieWithReleaseDates
    .withColumn("Today", current_date())
    .withColumn("Right_Now", current_timestamp())
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365 ) // date_add, date_sub

  movieWithReleaseDates.select("*").where(col("Actual_Release").isNull).show()


  /*
  * Exercise how to deal with multiple date format
  *
  *
  * */
  // - parse DF multiple time

  val stocksDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/stocks.csv")


  val stockDFWithDdates = stocksDF.withColumn("actual_date", to_date(col("date"), "MMM dd yyyy"))
  stockDFWithDdates.show()

  // Structures

  // 1 with column operators
  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"), col("Worldwide_Gross")).as("Profit")
  ).select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF.selectExpr(
    "Title",
    "(US_Gross, Worldwide_Gross) as Profit"
  ).selectExpr("Title", "Profit.US_Gross")


  // Arrays
  val moviessWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // array of strings
  moviessWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  )







}
