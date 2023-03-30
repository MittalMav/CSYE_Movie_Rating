import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import scala.util.{Try, Success, Failure}

/*
These test cases check if the read_file method returns a DataFrame and handles errors correctly,
if the analyze_stats method returns the correct mean and standard deviation of the rating column,
and if an error is returned if an incorrect math function is passed to analyze_stats.
 */

class MovieRatingAnalyzerTest extends AnyFlatSpec with Matchers {
  val spark = SparkSession.builder()
    .appName("MovieRatingAnalyzerTest")
    .master("local[*]")
    .getOrCreate()

  val filePath = "src/test/resources/movie_metadata.csv"
  val analyzer = MovieRatingAnalyzer(filePath)

  "The MovieRatingAnalyzer" should "read a CSV file and return a DataFrame" in {
    val df = analyzer.read_file(spark)
    df.isSuccess shouldBe true
    df.get.count() shouldBe 1609
  }

  it should "return an error if the file path is incorrect" in {
    val analyzerWrongPath = MovieRatingAnalyzer("src/test/resources/wrong_path.csv")
    analyzerWrongPath.read_file(spark).isFailure shouldBe true
  }

  it should "return the correct mean and standard deviation of the rating column" in {
    val df = analyzer.read_file(spark).get
    val mean_df = analyzer.analyze_stats(df, "imdb_score", "mean")
    val std_df = analyzer.analyze_stats(df, "imdb_score", "std")

    mean_df.isSuccess shouldBe true
    f"%%.4f".format(mean_df.get.first().getDouble(0)).toDouble shouldBe 6.4532
    std_df.isSuccess shouldBe true
    f"%%.4f".format(std_df.get.first().getDouble(0)).toDouble shouldBe 0.9988
  }

  it should "return an error if an incorrect math function is passed" in {
    analyzer.analyze_stats(spark.emptyDataFrame, "imdb_score", "wrong").isFailure shouldBe true
  }


}
