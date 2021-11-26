package movie

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter
import book.MovieAnalyzer

object Movie {

  private val toyStoryId = 32

  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Beeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {

      val movieAnalyzer = new MovieAnalyzer(spark)

      val json = ujson.Obj(
        "Toy Story" -> movieAnalyzer.movieRatingAmounts(toyStoryId),
        "hist_all" -> movieAnalyzer.ratingAmounts
      )

      writeFile("src/main/resources/result.json", json.toString())

    } finally {
      spark.close
    }
  }

  /** write a `String` to the `filename`.
    */
  def writeFile(filename: String, s: String): Unit = {
    val file = new File(filename)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(s)
    bw.close()
  }
}
