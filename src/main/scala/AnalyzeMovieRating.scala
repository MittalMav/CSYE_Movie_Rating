import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.{Try,Success,Failure}
import org.apache.spark.sql.functions.{avg, stddev}

case class MovieRatingAnalyzer(filePath: String) {

  // Method to get & process DataFrame & return mean & stddev
  def analyze_stats(df: DataFrame, col: String, stats: String): Try[DataFrame] = {
    stats match {
      case "mean" => Success(df.select(avg(col)))
      case "std" => Success(df.select(stddev(col)))
      case _ => Failure(new Exception("Please pass correct Math function"))
    }
  }

  def read_file(ss: SparkSession): Try[DataFrame] = {
    Try(ss.read.format("csv").option("header","true").load(filePath))
  }

}

object AnalyzeMovieRating extends App{
  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("AnalyzeMovieRating")
    .getOrCreate()
//  println("starting")
//  println(spark)
//  println("Spark Version : " + spark.version)
//  println("ending")
  spark.sparkContext.setLogLevel("ERROR")
  val filePath = "D:/Scala/Movie_Rating/src/test/resources/movie_metadata.csv"
  val analyzer = MovieRatingAnalyzer(filePath)
 // val df = spark.read.format("csv").option("header","true").load(filePath)
  val df = {
    analyzer.read_file(spark) match {
      case Success(df) => df
      case Failure(f) => throw new Exception(s"*****Error***** Exception $f")
    }
  }
  df.show()

  val mean_df = analyzer.analyze_stats(df, "imdb_score", "mean")
  val std_df = analyzer.analyze_stats(df, "imdb_score", "std")

  println(df.count())
  println(mean_df)

  val mean = mean_df match{
    case Success(df) => f"%%.4f".format(df.first().getDouble(0))
    case Failure(ex) => ex
  }

  val std = std_df match {
    case Success(df) => f"%%.4f".format(df.first().getDouble(0)).toDouble
    case Failure(ex) => ex
  }

  println(s"The mean value of rating is: $mean")
  println(s"The standard deviation of ratings is: $std")

}
