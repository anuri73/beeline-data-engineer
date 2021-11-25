package book

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Book {

  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Beeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {

      import spark.implicits._

      val books = spark.read
        .option("header", true)
        .option("inferSchema", true)
        .csv("src/main/resources/books.csv")

      val dataFrameSchema = books.schema

      println(Console.GREEN + s"Schema: {$dataFrameSchema}" + Console.RESET)

      val totalAmount = books.count()

      println(
        Console.RED + s"Total amount is: $totalAmount." + Console.RESET
      )

      val highRatingBooks = books.filter("average_rating >= 4.5")

      val highRatingBooksCount = highRatingBooks.count()

      println(
        Console.YELLOW + s"Total amount of high rating books is: $highRatingBooksCount." + Console.RESET
      )

      val averageRating = books.select(mean("average_rating")).collect()(0)(0)

      println(
        Console.BLUE + s"Average book rating is: $averageRating." + Console.RESET
      )

    } finally {
      spark.close
    }
  }
}
