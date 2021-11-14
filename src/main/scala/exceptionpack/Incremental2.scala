package exceptionpack
//import erick1.spark
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, lit, max, rank}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions. {col}
import java.sql.Date
import java.text._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.Window.orderBy

case class CustomerUpdate(customerId: Int, address: String, effectiveDate: Date)
case class Customer(customerId: Int, address: String, effectiveDate: Date, endDate: Date)

object Incremental2 {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("myincrementalapp2").master("local[*]")
      .config("spark.sql.shuffle.partitions", "1")
      .config ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .getOrCreate()
    val sc = spark.sparkContext

    implicit def date(str: String): Date = Date.valueOf(str)
    import spark.implicits._
println ("1. original table")
    val df = Seq(Customer(1, "Denver", null, "2018-02-01"),
      Customer(1, "New York", "2018-02-02", null),
      Customer(2, "Budapest", "2018-02-02", null),
      Customer(3, "London", "2018-02-02", null)).toDF()
     /*df.show()*/
    df.coalesce(1)
      .write
       .format("com.databricks.spark.csv")
       .option("header", "true")
       .mode("overwrite")
       .save("D:\\proj2\\src\\main\\scala\\exceptionpack\\output")
println ("2. updates table")
    val updatesDF = Seq(
    CustomerUpdate(1, "San Francisco", "2020-01-20"),
    CustomerUpdate(3, "India", "2020-01-20"), // new address same as current address for customer 3
    CustomerUpdate(4, "Berlin", "2020-01-20")
  ).
    toDF()
  //updatesDF.createOrReplaceTempView("updates")
    updatesDF.show()

    val uni = updatesDF.withColumn("endDate", lit (""))
    val unio = df.union (uni)
    println ("3. Before grouby")
    unio.orderBy ("customerId","effectiveDate").show()

    println ("4. unioned table ALL")
    val gr = unio.groupBy("customerId", "address").agg(max($"effectiveDate").as("effectiveDate"))
    val unioned = gr.join (unio, Seq ("customerId", "address","effectiveDate"))
    unioned.show()

    println ("5. joined table ALL")
    val joined = updatesDF.join (df,Seq ("customerId", "address","effectiveDate"),"outer").dropDuplicates("address")
    joined.groupBy("customerId", "address","effectiveDate").agg(max($"effectiveDate")).show()

   /* println ("4. Windowed table")
    val byName = Window.partitionBy("customerId", "address").orderBy('effectiveDate)
    val byName1 = joined.withColumn("rank_by_name", rank().over(byName))
    byName1.show()*/


    //table with schema (customerId, address, current, effectiveDate, endDate)
    /*import io.delta.tables._
    val customersTable: DeltaTable =
    DeltaTable.forPath("D:\\proj2\\src\\main\\scala\\exceptionpack\\output")
  println("Existing data")
  customersTable.toDF.show(10, false)*/
  // Rows to INSERT new addresses of existing customers
  /*val newAddressesToInsert = updatesDF.
    as("updates").
    join(customersTable.toDF.as("customers"), "customerid").
    where("customers.current = true AND updates.address <> customers.address")
  println("New records to insert")
  newAddressesToInsert.show(10, false)

  // Stage the update by unioning two sets of rows
  // 1. Rows that will be inserted in the `whenNotMatched` clause
  // 2. Rows that will either UPDATE the current addresses of existing customers or INSERT the new addresses of new customers
  val stagedUpdates = newAddressesToInsert.
    selectExpr("NULL as mergeKey", "updates.*").   // Rows for 1.
    union(
      updatesDF.as("updates").selectExpr("updates.customerId as mergeKey", "*")  // Rows for 2.
    )

  println("Updates to perform")
  stagedUpdates.show(10, false)

  // Apply SCD Type 2 operation using merge
  customersTable.
    as("customers").
    merge(
      stagedUpdates.as("staged_updates"),
      "customers.customerId = mergeKey").
    whenMatched("customers.current = true AND customers.address <> staged_updates.address").
    updateExpr(Map(  // Set current to false and endDate to source's effective date.
      "current" -> "false",
      "endDate" -> "staged_updates.effectiveDate")).
    whenNotMatched().
    insertExpr(Map(
      "customerid" -> "staged_updates.customerId",
      "address" -> "staged_updates.address",
      "current" -> "true",
      "effectiveDate" -> "staged_updates.effectiveDate", // Set current to true along with the new address and its effective date.
      "endDate" -> "null")).
    execute()

  // View the saved data
  DeltaTable.forPath("D:\\proj2\\src\\main\\scala\\exceptionpack\\output").toDF.show
*/
  }

}
