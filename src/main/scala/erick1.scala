import erick1.df_joined_1
import exceptionpack.Erickexc
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, collect_set, count, explode, regexp_replace, size, sum, when}
import org.apache.spark.storage.StorageLevel._

import scala.util.{Failure, Success, Try}
//import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger, LogManager}
//import gkfunctions.read_schema

object erick1 extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  //def main(args: Array[String]): Unit = {
  val spark = SparkSession.builder().appName("myapp1").master("local[*]") //
    .config("spark.sql.autoBroadcastJoinThreshold", "1")
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .config("spark.sql.defaultSizeInBytes", "100000")
    .getOrCreate()
  val sc = spark.sparkContext

  val df_cust = spark.read.format("jdbc") //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc") //  <= connection string
    .option("driver", "com.mysql.jdbc.Driver") //   <= Driver class
    .option("dbtable", "customers_table") //    <=database table name
    .option("user", "root") //
    .option("password", "") //
    .load()
  val df_ord_det = spark.read.format("jdbc") //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc") //  <= connection string
    .option("driver", "com.mysql.jdbc.Driver") //   <= Driver class
    .option("dbtable", "orderdetails") //    <=database table name
    .option("user", "root") //
    .option("password", "") //
    .load()
  //df_ord_det.show(5)

  val df_orders = spark.read.format("jdbc") //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc") //  <= connection string
    .option("driver", "com.mysql.jdbc.Driver") //   <= Driver class
    .option("dbtable", "orders_table") //    <=database table name
    .option("user", "root") //
    .option("password", "") //
    .load()
  //df_orders.show(5)


  val df1 = df_cust.count()
  // excuse me
  val df2 = df_cust.columns
  println(df1, df2.size)

    //df.write.format("orc").mode("update").save("D:\\proj2\\output")
    //df2.write.format("csv").mode("overwrite").save("D:\\proj2\\output1")


  //val df3 = df_cust.repartition (1)
  //df.write.format("delta").mode("update").save("D:\\proj2\\output")
  //df_cust.show(5)
  def validation(x: DataFrame): DataFrame = {
    if (x.dropDuplicates(Seq("address", "postalcode", "city")).count() != x.count()) {
      throw new Erickexc("duplicates found on these columns")
    }
    x
  }
  //validation(df_cust).show()
  //val df_fin = Try(validation(df_cust))
  //df_fin match {
    //case Success(x) => x.show()
   // case Failure(exception) => println(s"Failed with error: ${exception.getMessage}")
 // }
  //df.show(5)
  //df_cust.printSchema()

  val df_joined = df_ord_det
    .join (df_orders, Seq ("orderid")).select ("customerid", "orderid", "orderdate", "productid", "quantity")

  val df_joined_1 = df_joined
    .join (df_cust, Seq ("customerid")).select ("customerid","contactname", "orderid", "quantity", "orderdate","country")

  df_joined_1.show(5)
  val df3 = df_joined_1.count()
  // excuse me
  val df4 = df_joined_1.columns
  println(df3, df4.size)
  //def replacing (y: DataFrame, colname: String, sourceword:String, replaceWith:String): DataFrame = {
   // val other = df.withColumn ("newcol", regexp_replace(df(col("address)),"\\w+", ".")
  //}
  //val new_df = replacing(df_cust, df_cust(col("contactname")),"Maria", "Ander")
  //new_df.show()
  import spark.implicits._
  //val colu = df4.toList

  /// filter null values on all columns
  val df_joined_2 = df_joined_1.columns
    .map(x =>col (x).isNull).reduce(_&&_)
  val filt = df_joined_1.filter(df_joined_2)
 val df5 = filt.count()
  println(df5)
  //df_joined_1.show()

  val df_pr = spark.read.format("jdbc") //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc") //  <= connection string
    .option("driver", "com.mysql.jdbc.Driver") //   <= Driver class
    .option("dbtable", "products_table") //    <=database table name
    .option("user", "root") //
    .option("password", "") //
    .load()
  //df_pr.show(5)
  println ("***Rollup table***")
  val df_prod1 = df_ord_det.rollup("orderid", "productid").agg(sum($"quantity").alias("qtty"))
  df_prod1.orderBy($"orderid", $"productid".desc_nulls_last).show()

  //Collect_set
  println ("***Collect set table***")
  val df_collect_set = df_joined_1.groupBy("contactname")
    .agg(collect_set("orderid").alias("list_orders"))

  df_collect_set.show(7)

  val df_fin = df_joined_1
     .groupBy("contactname").agg(count("orderid").alias("ord_ct"))
  df_fin.show(5)

  val df_finals = df_fin.join (df_fin, Seq ("contactname"))
  df_finals.show(5)
  df_finals.printSchema()
// Unable to write results of collect_set. It says Array issue

  /*df_finals.write.format("jdbc")
    .options(Map("url"->"jdbc:mysql://localhost:3306/gkc",
  "driver"->"com.mysql.jdbc.Driver",
  "dbtable" ->"df_finals_col_set",
  "user"  ->"root",
  "password" -> ""))
  .mode("append").save()*/

  //df_finals.write.format("text")
   // .mode("overwrite").save("D:\\proj2\\output\\finals")

}
