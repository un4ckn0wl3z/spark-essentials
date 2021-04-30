package apracticepart2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import utils.DataFrameUtil


object Joins extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("DF Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read.option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  val joinCondition = guitaristDF.col("band") === bandsDF.col("id")

  val guitaristsBandDF = guitaristDF.join(bandsDF, joinCondition, "inner")
  guitaristsBandDF.show()

  // outer joins
  // left-outer = contain everything in the inner join + rows in the LEFT table with null where the data missing
  guitaristDF.join(bandsDF, joinCondition, "left_outer").show()

  // right-outer = contain everything in the inner join + rows in the RIGHT table with null where the data missing
  guitaristDF.join(bandsDF, joinCondition, "right_outer").show()

  // outer = contain everything in the inner join + rows in the BOTH tables with null where the data missing
  guitaristDF.join(bandsDF, joinCondition, "outer").show()


  // semi-joins - only left with match condition
  guitaristDF.join(bandsDF, joinCondition, "left_semi").show()

  // anti-joins -> row in left that not match the condition
  guitaristDF.join(bandsDF, joinCondition, "left_anti").show()

  // bear in mind
  // *** guitaristsBandDF.select("id", "band") // this crash

  // how to solve
  // - 1.rename the columns
  guitaristDF.join(bandsDF.withColumnRenamed("id", "band"), "band").show()

  // - 2.drop the dup
  guitaristsBandDF.drop(bandsDF.col("id")).show()

  // - 3 rename the offending columns and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  //bandsModDF.show()
  guitaristDF.join(bandsModDF, guitaristDF.col("band") === bandsModDF.col("bandId")).show()

  // using complex type
  guitaristDF.join(guitarsDF.withColumnRenamed("id", "guitarId")
    , expr("array_contains(guitars, guitarId)"))

  bandsDF.show()

//  bandsDF
//    .write
//    .format("com.databricks.spark.csv")
//    .options(
//      Map[String, String](
//        ("header","true"),
//        ("delimiter","\t")
//      )
//    )
//    .mode("overwrite")
//    .save("src/main/resources/data/bands1.csv")




  DataFrameUtil.saveCsvAsSingFile(
    bandsDF,"csv",
    spark.sparkContext,
    "src/main/resources/data/bands1/tmp",
    "src/main/resources/data/bands1/myfile.txt",
    "overwrite",
    Map[String, String](
      ("header","true"),
      ("delimiter","\t")
    )
  )



}
