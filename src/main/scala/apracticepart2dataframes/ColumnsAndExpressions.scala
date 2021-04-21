package apracticepart2dataframes
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

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

  carsDF.show()

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
  selectedDF.show()
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

  carsWithWeightDF.show()

  // select expr
  val carsWithSelectExprWeight = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  carsWithSelectExprWeight.show()

  // DF processiong
  // adding columns
  val carsWithKg = carsDF.withColumn("Weight_in_Kg_3", col("Weight_in_lbs") / 2.2)
  carsWithKg.show()

  //rename
  val carsWithColumnsRename = carsDF.withColumnRenamed("Weight_in_lbs", "Weight_in_pounds")
  carsWithColumnsRename.show()
  val carsDfWithDrop = carsWithColumnsRename.drop("Cylinders", "Displacement")
  carsDfWithDrop.show()

  // filtering

  val europeCars = carsDF.filter(col("Origin") =!= "USA") // equal where
  europeCars.show()

  val actualEuropeCars = europeCars.where(col("Origin") =!= "Japan") // equal where
  actualEuropeCars.show()

  // filtering with expression string
  val americanCarsDF = carsDF.filter("Origin = 'USA'")
  americanCarsDF.show()

  // chain filter
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCarsDF2 = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCarsDF3 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF4 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  americanPowerfulCarsDF.show()
  americanPowerfulCarsDF2.show()
  americanPowerfulCarsDF3.show()
  americanPowerfulCarsDF4.show()

  // unions = adding more row

  val moreCarsDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/more_cars.json")

  val allCarDF = carsDF.union(moreCarsDF) // work if DFs have the same schema

  allCarDF.show() //

  // distinct
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()











}
