import exceptionpack.Erickexc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, when}

import scala.util.{Failure, Success, Try}
//import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger, LogManager}
//import gkfunctions.read_schema

object erick1 extends App {
  //def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().appName("myapp1").master("local[*]") //
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .config("spark.sql.defaultSizeInBytes", "100000")
    .getOrCreate()
  val sc = spark.sparkContext

  import spark.implicits._

  sc.setLogLevel("ERROR")

  val df = spark.read.format("jdbc") //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc") //  <= connection string
    .option("driver", "com.mysql.jdbc.Driver") //   <= Driver class
    .option("dbtable", "customers_table") //    <=database table name
    .option("user", "root") //
    .option("password", "") //
    .load()
  val df1 = df.count()
  // excuse me
  val df2 = df.columns
  println(df1, df2.size)

  //val df3 = df.repartition (1)
  //df3.write.format("orc").mode("update").save("D:\\proj2\\output")
  //df.show()
  def validation(x: DataFrame): DataFrame = {
    if (x.dropDuplicates(Seq("address", "postalcode", "city")).count() == x.count()) {
      throw new Erickexc("duplicates found on these columns")
    }
    x
  }

  //validation(df).show()
  val df_fin = Try(validation(df))
  df_fin match {
    case Success(x) => x.show()
    case Failure(exception) => println(s"Failed with error: ${exception.getMessage}")
  }
}