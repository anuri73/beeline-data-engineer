package book

import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ArrayBuffer

class MovieSuite extends munit.FunSuite {
  val spark = SparkSession
    .builder()
    .appName("Beeline")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val movieAnalyzer = new MovieAnalyzer(spark)

  test("ToyStory rating amount must be equal to specific value") {
    assertEquals(
      movieAnalyzer.movieRatingAmounts(32),
      ArrayBuffer[Long](3, 6, 19, 30, 23)
    )
  }

  test("All rating amount must be equal to specific value") {
    assertEquals(
      movieAnalyzer.ratingAmounts,
      ArrayBuffer[Long](6110, 11370, 27145, 34174, 21201)
    )
  }
}
