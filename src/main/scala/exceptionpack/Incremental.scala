package exceptionpack
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.functions.{col, regexp_replace, when}
import org.apache.spark.storage.StorageLevel._

import java.sql.Date
import scala.util.{Failure, Success, Try}
//import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger, LogManager}

object Incremental {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("myincrementalapp").master("local[*]") //
      .config("spark.sql.autoBroadcastJoinThreshold", "1")
      .config("spark.sql.join.preferSortMergeJoin", "false")
      .config("spark.sql.defaultSizeInBytes", "100000")
      .getOrCreate()
    val sc = spark.sparkContext

    def createDF(rows: Seq[Row], schema: StructType): DataFrame = {
      spark.createDataFrame(sc.parallelize(rows), schema)
    }
    val schema = StructType(
      List(
        StructField("order_no", StringType, true),
        StructField("customer_id", StringType, true),
        StructField("quantity", IntegerType, true),
        StructField("cost", DoubleType, true),
        StructField("order_date", DateType, true),
        StructField("last_updated_date", DateType, true)
      )
    )
    // Create orders dataframe
    val orders = Seq(Row(
                       "001", "c1", 1, 15.00, Date.valueOf("2020-03-01"), Date.valueOf("2020-03-01")),
                     Row(
                       "002", "c2", 1, 30.00, Date.valueOf("2020-04-01"), Date.valueOf("2020-04-01")),
      Row(
        "003", "c3", 2, 50.00, Date.valueOf("2020-04-08"), Date.valueOf("2020-04-01")),
      Row(
        "004", "c3", 13, 1000.00, Date.valueOf("2020-04-09"), Date.valueOf("2020-04-01")),
      Row(
        "005", "c4", 10, 500.00, Date.valueOf("2020-04-10"), Date.valueOf("2020-04-01")),
      Row(
        "006", "c5", 15, 20.00, Date.valueOf("2020-04-11"), Date.valueOf("2020-04-01"))
    )
    val ordersDF = createDF(orders, schema)
    ordersDF.show()

    // Create order_updates dataframe
    val orderUpdates = Seq(Row(
                      "002", "c2", 1, 20.00, Date.valueOf("2020-04-01"), Date.valueOf("2020-04-02")),
      Row(
        "003", "c3", 22, 50.00, Date.valueOf("2020-04-08"), Date.valueOf("2020-04-10")),
      Row(
        "004", "c3", 13, 1111.00, Date.valueOf("2020-04-09"), Date.valueOf("2020-04-10")),
      Row(
        "005", "c4", 10, 555.00, Date.valueOf("2020-04-10"), Date.valueOf("2020-04-11"))
    )
    val orderUpdatesDF = createDF(orderUpdates, schema)
    orderUpdatesDF.show()
    //orderUpdatesDF.except(ordersDF).orderBy("order_no").select ("order_no", "quantity", "cost").show()
    // Register temporary views
    ordersDF.createOrReplaceTempView("orders")
    orderUpdatesDF.createOrReplaceTempView("order_updates")

    val reconciled = orderUpdatesDF.union(ordersDF).dropDuplicates (Seq ("order_no", "customer_id"))
      .orderBy("order_no")
    reconciled.show()

  }
}
