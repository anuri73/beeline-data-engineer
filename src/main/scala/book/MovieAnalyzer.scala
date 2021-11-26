package book

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.collection.mutable.ArrayBuffer

class MovieAnalyzer(val spark: SparkSession) {
  import spark.implicits._

  private val src: String = "src/main/resources/ml-100k/u.data"

  var movies = spark.read
    .option("inferSchema", true)
    .textFile(src)

  private def ratings() = movies
    .map(row => row.split("\t"))
    .map(row => (row(1).toInt, row(2).toInt))
    .cache()

  def movieRatingAmounts(movieId: Int): Seq[Long] = ratings()
    .filter(row => row._1 == movieId)
    .groupByKey(_._2)
    .count()
    .collect()
    .toSeq
    .sortBy(_._1)
    .map(_._2)

  def ratingAmounts: Seq[Long] = ratings()
    .groupByKey(_._2)
    .count()
    .collect()
    .toSeq
    .sortBy(_._1)
    .map(_._2)
}
