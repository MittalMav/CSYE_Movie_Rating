
import org.apache.spark.sql.SparkSession
object version extends App {
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("SparkByExamples")
    .getOrCreate()
  println("starting")
  println(spark)
  println("Spark Version : "+spark.version)
  println("ending")
  spark.sparkContext.setLogLevel("ERROR")
}
