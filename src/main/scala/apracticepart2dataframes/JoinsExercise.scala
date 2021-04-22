package apracticepart2dataframes

import apracticepart2dataframes.DataSources.spark
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{SparkSession, functions}

object JoinsExercise extends App {

  // create a SparkSession
  System.setProperty("hadoop.home.dir", "C:\\Users\\anuwat\\Documents\\hadoop-2.8.1")
  val spark = SparkSession.builder()
    .appName("DF Joins")
    .config("spark.master", "local")
    .getOrCreate()

  // reading from db

  def readTable(tableName: String) = {
    spark.read
      .format("jdbc")
      .option("driver", "org.postgresql.Driver")
      .option("url","jdbc:postgresql://128.199.85.229:5432/rtjvm")
      .option("user","docker")
      .option("password","docker")
      .option("dbtable",s"public.$tableName")
      .load()
  }

  val employeesDF = readTable("employees")
  val salariesDF = readTable("salaries")
  val deptManagersDF = readTable("dept_manager")
  val titleDF = readTable("titles")

  // show all employees and their max salary
  val maxSalariesPerEmpNoDF = salariesDF.groupBy("emp_no").agg(
    functions.max("salary").as("maxSalary")
  )
  val employeesSalariesDF = employeesDF.join(maxSalariesPerEmpNoDF, "emp_no")
  employeesSalariesDF.show()

  // 2 show all employees who were never managers
  val empNeverManagersDF = employeesDF.join(deptManagersDF
    , employeesDF.col("emp_no") === deptManagersDF.col("emp_no")
    , "left_anti")

  empNeverManagersDF.show()

  // 3
  val mostRecentJobTitleDF = titleDF.groupBy("emp_no", "title").agg(
    functions.max("to_date")
  )
  val bestPaidEmployeesDF = employeesSalariesDF.orderBy(col("maxSalary").desc).limit(10)
  val bestPaidJobDF = bestPaidEmployeesDF.join(mostRecentJobTitleDF, "emp_no")

  bestPaidJobDF.show()

}
