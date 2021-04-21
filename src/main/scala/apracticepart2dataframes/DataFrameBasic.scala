package apracticepart2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

import scala.reflect.internal.util.NoFile.file

object DataFrameBasic extends App {
  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("DataFrames Basic")
    .config("spark.master", "local")
    .getOrCreate()



  // reading a DF

  val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")


  // showing a dataframe
  firstDF.show()

  // print schema
  firstDF.printSchema()

  // get rows
  firstDF.take(10).foreach(println)

  // spark type
  val longType = LongType

  // dataframe schema
  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  // obtain schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read dataframe with own scheme

  val carsDFwithOwnScheme = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create DF from tuple
  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // auto -infer, no columns name

  // df has schema, row not
  // create df with implicits

  import spark.implicits._
  val manualCarsDFwithImplicit = cars.toDF("Name",
    "MPG", "Cylinders", "Displacement", "HP", "Weight", "Acceleration", "Year", "Country")

  manualCarsDF.printSchema()
  manualCarsDFwithImplicit.printSchema()

  /**
    * 1. Create a manual DF describing smartphone
    *  - make
    *  - model
    *  - screen
    *  - camera mgpx
    *
    * 2. Read another file from data folder -> movies.json
    *  - print
    *  - count
    *
    *
    */

  val smartphone = Seq(
    ("Samsung", "Galaxy", "Android", 12),
    ("Apple", "iPhone X", "iOS", 13),
    ("Nokia", "3310", "THE BEST", 0),
  )

  val smartphoneDF = smartphone.toDF("Make", "Model", "Platform", "CameraMegaPixel")
  smartphoneDF.show()

  val moviesDF = spark.read.format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  moviesDF.printSchema()
  println(moviesDF.count() + " rows")









}
