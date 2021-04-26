package apracticepart3typesanddatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, initcap, lit, not, regexp_extract, regexp_replace}
import org.apache.spark.sql.types.FloatType

object CommonTypes extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("Common Spark Types")
    .config("spark.master", "local")
    .getOrCreate()



  val moviesDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  // adding a common plain value to DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Boolean
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter
  moviesDF.select("Title").where(preferredFilter)
  // + multiple ways of filtering

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  //moviesWithGoodnessFlagsDF.where(col("good_movie") === false ) .show()// where by good_movie === True
  moviesWithGoodnessFlagsDF.where("good_movie")// where by good_movie === True

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))// where by good_movie === false

  // Numbers
  // math opers
  val moviesAvgRatingsDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 10 + col("IMDB_Rating")) / 2)
  moviesAvgRatingsDF.show()

  // correlation = number between -1 and 1
  // println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating") /*corr is ACTION */)

  // String

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  // capitalozation:. initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen")).show()

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract")

  // replacing
  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  ).drop("Name").show()

  /*
  * Exercise
  * - filter the cars DF by a list of car names obtained by an API call
  * --- version -> contain, regex
  * */

  def getCarNames: List[String] = {
    List("Volkswagen", "Mercedes-Benz", "Ford")
  }


  // 1 - regex
  val complexRegex = getCarNames.map(_.toLowerCase()).mkString("|")
  carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), complexRegex, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "").drop("regex_extract").show()

  // 2 - contains
  val carsNameFilters = getCarNames.map(_.toLowerCase()).map(name => col("Name").contains(name))
  val bigFilter = carsNameFilters.fold(lit(false))((combindedFilter, newCarNameFilter) => {
    combindedFilter or newCarNameFilter
  } )
  carsDF.filter(bigFilter).show()












}
