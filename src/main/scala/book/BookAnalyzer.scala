package book

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

class BookAnalyzer(val spark: SparkSession) {

  import spark.implicits._

  private val src: String = "src/main/resources/books.csv"

  private val books = spark.read
    .option("header", true)
    .option("inferSchema", true)
    .csv(src)

  def bookSchema() = books.schema

  def totalAmount(): Int = books.count().toInt

  def highRatingBooksAmount(): Int =
    books.filter("average_rating >= 4.5").count().toInt

  def averageRating(): Double = {
    val average = books.select(mean("average_rating")).collect()(0)(0)
    average match {
      case n: java.lang.Number => n.doubleValue()
      case _ => throw new RuntimeException("Unable to parse double value")
    }
  }

}
