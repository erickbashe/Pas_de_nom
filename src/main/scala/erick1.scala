import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType, TimestampType, DoubleType}
import org.apache.spark.sql.functions.{col, when}
//import com.typesafe.config.{Config, ConfigFactory}
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import org.apache.log4j.{Level, Logger, LogManager}
//import gkfunctions.read_schema

object erick1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("myapp1").master("local[*]")//
      .config("spark.sql.autoBroadcastJoinThreshold", "1")
      .config("spark.sql.join.preferSortMergeJoin", "false")
      .config("spark.sql.defaultSizeInBytes", "100000")
      .getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    sc.setLogLevel("WARN")

    val df = spark.read.format("jdbc")   //<=format is jdbc
    .option("url", "jdbc:mysql://localhost:3306/gkc")//  <= connection string
    .option("driver", "com.mysql.jdbc.Driver")//   <= Driver class
    .option("dbtable", "customers_table") //    <=database table name
    .option("user", "root")//
    .option("password", "")//
    .load()
    val df1 = df.count()
    // excuse me
    val df2 = df.columns
    println(df1, df2.size)

    df.write.format("orc").mode("overwrite").save("D:\\proj2\\output")

    df.show()
  }

}
