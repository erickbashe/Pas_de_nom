import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession
import org.apache.log4j.{Level, LogManager, Logger}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel._
object erick3 {
  Logger.getLogger("org").setLevel(Level.OFF)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("myapp1").master("local[*]").getOrCreate()
    val configs: Config = ConfigFactory.load("application.conf")
    val in1 = configs.getString("paths.inputLocation")
    val input = spark.read.csv(in1 + "data1.csv")
  }

}
