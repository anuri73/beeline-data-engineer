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

      val bookAnalyzer: BookAnalyzer = new BookAnalyzer(spark)

      println(
        Console.GREEN + s"Schema: ${bookAnalyzer.bookSchema}" + Console.RESET
      )

      println(
        Console.RED + s"Total amount is: ${bookAnalyzer.totalAmount()}." + Console.RESET
      )

      println(
        Console.YELLOW
          + s"Total amount of high rating books is: ${bookAnalyzer.highRatingBooksAmount()}."
          + Console.RESET
      )

      println(
        Console.BLUE + s"Average book rating is: ${bookAnalyzer.averageRating()}." + Console.RESET
      )

    } finally {
      spark.close
    }
  }
}
