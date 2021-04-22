package apracticepart2dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, column, count, countDistinct, expr, mean, min, stddev, sum}

object ColumnsAndExpressions extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("DF ColumnsAndExpressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

 // carsDF.show()

  // Colums
//  val firstColumns = carsDF.col("Name")
//
//  // selecting (projection)
//  val carNamesDF = carsDF.select(firstColumns)
//  carNamesDF.show()

  // various select method
//  import spark.implicits._
//  carsDF.select(
//    carsDF.col("Name"),
//    col("Acceleration"),
//    column("Weight_in_lbs"),
//    'Year,
//    $"Horsepower",
//    expr("Origin")
//  )

  // select with plain columns name
  val selectedDF = carsDF.select("Name", "Year")
 // selectedDF.show()
  selectedDF.printSchema()

  // EXPRESSIONS

  val simplestExpress = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_Kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_lbs_2")
  )

 // carsWithWeightDF.show()

  // select expr
  val carsWithSelectExprWeight = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  //carsWithSelectExprWeight.show()

  // DF processiong
  // adding columns
  val carsWithKg = carsDF.withColumn("Weight_in_Kg_3", col("Weight_in_lbs") / 2.2)
  //carsWithKg.show()

  //rename
  val carsWithColumnsRename = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  //carsWithColumnsRename.show()
  val carsDfWithDrop = carsWithColumnsRename.drop("Cylinders", "Displacement")
 // carsDfWithDrop.show()

  // filtering

  val europeCars = carsDF.filter(col("Origin") =!= "USA") // equal where
  //europeCars.show()

  val actualEuropeCars = europeCars.where(col("Origin") =!= "Japan") // equal where
  //actualEuropeCars.show()

  // filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  //americanCarsDF.show()

  // chain filter
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

//  americanPowerfulCarsDF.show()
//  americanPowerfulCarsDF2.show()
//  americanPowerfulCarsDF3.show()
//  americanPowerfulCarsDF4.show()

  // unions = adding more row

  val moreCarsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/more_cars.json")

  val allCarDF = carsDF.union(moreCarsDF) // work if DFs have the same schema

  //allCarDF.show() //

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  //allCountriesDF.show()


  /*
  * Exercise
  *
  * */

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()

  val moviesReleaseDateDF = moviesDF.select("Title", "Release_Date")
  moviesReleaseDateDF.show()

  val moviesProfitDF = moviesDF.withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))
  moviesProfitDF.show()

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross as Total_Gross"
  )

  moviesProfitDF2.show()

  val comedyMoviesDF = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)
  comedyMoviesDF.show()


  val comedyMoviesDF2 = moviesDF
    .select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comedyMoviesDF3 = moviesDF
    .select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comedyMoviesDF.show()
  comedyMoviesDF2.show()
  comedyMoviesDF3.show()


  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  val minRatingDF2 = moviesDF.selectExpr("min(IMDB_Rating)")

  minRatingDF2.show()

  // sum
  val sumUSGrossDF = moviesDF.select(sum(col("US_Gross")))
  val sumUSGrossDF2 = moviesDF.selectExpr("sum(US_Gross)")
  sumUSGrossDF2.show()

  // avg
  val avgRateDF = moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  val avgRateDF2 = moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")
  avgRateDF2.show()

  // data sci
  val dataSciDF = moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  dataSciDF.show()

  // Grouping
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")) // include null
    .count() // select count(*) from moviesDF group by Major_Genre

  countByGenreDF.show()

  val avgRatingGenreDF =  moviesDF.groupBy(col("Major_Genre")) // include null
    .avg("IMDB_Rating")
  avgRatingGenreDF.show()

  // another way
  val aggregationsByGenreDF = moviesDF.groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  aggregationsByGenreDF.show()

  /*** exercise **/

  //1

  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross")).show()

  //2
  moviesDF.select(countDistinct(col("Director"))).show()

  moviesDF.select(col("Director")).distinct().show()
  moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross")
  ).show()

  //
  moviesDF.groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    ).orderBy(col("Avg_Rating").desc_nulls_last).show()


















}
