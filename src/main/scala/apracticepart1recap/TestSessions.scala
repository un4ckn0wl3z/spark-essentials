package apracticepart1recap

import org.apache.spark.sql.SparkSession

object TestSessions extends App {

  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark1 = SparkSession.builder()
    .appName("spark1")
    .config("spark.master", "local")
    .getOrCreate()

  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark2 = SparkSession.builder()
    .appName("spark2")
    .config("spark.master", "local")
    .getOrCreate()


  val moviesDF = spark1.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/movies.json")

  val carsDF = spark2.read
    .format("json")
    .option("mode", "failFast") //
    .option("path", "src/main/resources/data/cars.json") //
    .load()


  moviesDF.show()
  carsDF.show()

}
