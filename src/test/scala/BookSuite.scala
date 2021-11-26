package book

import org.apache.spark.sql.SparkSession

class BookSuite extends munit.FunSuite {

  val spark = SparkSession
    .builder()
    .appName("Beeline")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val bookAnalyzer = new BookAnalyzer(spark)

  test("'bookSchema' should return specific value") {
    assertEquals(
      bookAnalyzer.bookSchema().toString(),
      "StructType(StructField(bookID,IntegerType,true), StructField(title,StringType,true), StructField(authors,StringType,true), StructField(average_rating,StringType,true), StructField(isbn,StringType,true), StructField(isbn13,StringType,true), StructField(language_code,StringType,true), StructField(num_pages,StringType,true), StructField(ratings_count,IntegerType,true), StructField(text_reviews_count,IntegerType,true), StructField(publication_date,StringType,true), StructField(publisher,StringType,true))"
    )
  }

  test("'totalAmount' must be equal to specific value") {
    assertEquals(
      bookAnalyzer.totalAmount(),
      11127
    )
  }

  test("'averageRating' must be equal to specific value") {
    assertEquals(
      bookAnalyzer.averageRating(),
      3.934075339386829
    )
  }
}
