package apracticepart3typesanddatasets


import org.apache.spark.sql.{Dataset, Encoders, SparkSession}
import org.apache.spark.sql.functions._

import java.sql.Date

object Datasets extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()



  val numbersDF = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/numbers.csv")


  // numbersDF.printSchema()
  // numbersDF.filter(col("numbers") > 100)

  // convert dataframe to dataset
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]
  numbersDS.filter(_ > 100)

  // dataset of a complex type
  // 1 - define your case class
  case class Car (
                   Name: String,
                   Miles_per_Gallon: Option[Double],
                   Cylinders: Long,
                   Displacement: Double,
                   Horsepower: Option[Long],
                   Weight_in_lbs: Long,
                   Acceleration: Double,
                   Year: Date,
                   Origin: String
                 )

  // 2 - read DF from file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"src/main/resources/data/$filename")
//
//
//
//  // implicit val carEncoder = Encoders.product[Car]
//
//  // define encoders
import spark.implicits._
//  val carsDF = readDF("cars.json")
//  val carsDS = carsDF.as[Car]
//
//  // DS collection functions
//  val carNameDS = carsDS.map(car => {
//    car.Name.toUpperCase()
//  })
//
//  carNameDS.show()

// another way
val carSchema = Encoders.product[Car].schema
  val newCarDF = spark.read
    .option("inferSchema", "true")
    .schema(carSchema)
    .json(s"src/main/resources/data/cars.json").as[Car]

  // newCarDF.show()

//    val carNameDS = newCarDF.map(car => {
//      car.Name.toUpperCase()
//    })
//  carNameDS.show()
  val carsCount = newCarDF.count
  println(carsCount)
  println(newCarDF.filter(_.Horsepower.getOrElse(0L) > 140).count())
  println(newCarDF.map(_.Horsepower.getOrElse(0L)).reduce(_ + _) / carsCount)


  val horsepowerAvgDS = newCarDF.select(avg(col("Horsepower")).as("Horsepower_Avg"))

  // also use the DF Functions
  newCarDF.select(avg(col("Horsepower")))

  // Joins
  case class Guitar ( id: Long, make: String, model: String, guitarType: String )
  case class GuitarPlayer ( id: Long, name: String, guitars: Seq[Long], band: Long )
  case class Band ( id: Long, name: String, hometown: String, year: Long )

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarsPlayerDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS = guitarsPlayerDS
    .joinWith(bandsDS, guitarsPlayerDS.col("band") === bandsDS.col("id"), "inner")

  guitarPlayerBandsDS
    .withColumnRenamed("_1", "players")
    .withColumnRenamed("_2", "bands")

  // Exercise: join the guitarsDS and guitarsPlayerDS in, an outer join
  guitarsPlayerDS
    .joinWith(guitarsDS,
      array_contains(guitarsPlayerDS.col("guitars"),
        guitarsDS.col("id")), "outer" )
    //.show()

  // grouping
  val carsGroupByOrigin = newCarDF.groupByKey(_.Origin).count().show()

  // joins and group are WIDE transformation, will involve SHUFFLE operations








}
