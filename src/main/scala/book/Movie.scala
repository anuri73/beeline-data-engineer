package movie

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io.File
import java.io.BufferedWriter
import java.io.FileWriter

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

      import spark.implicits._

      var movies = spark.read
        .option("inferSchema", true)
        .textFile("src/main/resources/ml-100k/u.data")

      val ratings = movies
        .map(row => row.split("\t"))
        .map(row => (row(1).toInt, row(2).toInt))

      val toyStory = ratings
        .filter(row => row._1 == toyStoryId)
        .groupByKey(_._2)
        .count()
        .collect()
        .toSeq
        .sortBy(_._1)
        .map(_._2)

      val all = ratings
        .groupByKey(_._2)
        .count()
        .collect()
        .toSeq
        .sortBy(_._1)
        .map(_._2)

      val json = ujson.Obj(
        "Toy Story" -> toyStory,
        "hist_all" -> all
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
