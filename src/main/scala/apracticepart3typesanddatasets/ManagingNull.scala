package apracticepart3typesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNull extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("ManagingNull")
    .config("spark.master", "local")
    .getOrCreate()
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // select the first non-null value

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"), col("IMDB_Rating") * 10)
  ).show()

  // checking for null
  moviesDF.select("*").where(col("Rotten_Tomatoes_Rating").isNull)

  // null when ordering
  moviesDF.orderBy(col("IMDB_Rating").desc_nulls_last)

  // remove or replace nulls
  moviesDF.select("Title", "IMDB_Rating").na.drop() // remove null

  // replace null
  moviesDF.na.fill(0, List("IMDB_Rating", "Rotten_Tomatoes_Rating"))

  moviesDF.na.fill(Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 10,
    "Director" -> "Unknown"
  ))

  // complex operations
  moviesDF.selectExpr(
    "Title",
    "IMDB_Rating",
    "Rotten_Tomatoes_Rating",
    "ifnull(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as ifnull", // same as coalesce
    "nvl(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nvl", // same as coalesce
    "nullif(Rotten_Tomatoes_Rating, IMDB_Rating * 10) as nullif",
    "nvl2(Rotten_Tomatoes_Rating, IMDB_Rating * 10, 0.0) as nvl2" // if (first != null) second else third
  ).show()



















}
