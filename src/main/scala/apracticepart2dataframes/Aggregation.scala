package apracticepart2dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, col, count, countDistinct}


object Aggregation extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("DF Aggregation")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // counting
  val genreCountDF = moviesDF.select(count(col("Major_Genre")))
  genreCountDF.show() // all the values except null

  val genreCountDF2 = moviesDF.selectExpr("count(Major_Genre)")
  genreCountDF2.show() // all the values except null

  val genreCountDF3 = moviesDF.select(count("*"))
  genreCountDF3.show() // all the values include null


  // count distinct value
  val uniqueGengreDF = moviesDF.select(countDistinct(col("Major_Genre")))
  uniqueGengreDF.show()


  // approximate count
  val approximateDF = moviesDF.select(approx_count_distinct(col("Major_Genre")))
  approximateDF.show()

















}
