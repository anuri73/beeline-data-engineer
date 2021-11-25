package movie

import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Movie {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("Beeline")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    try {

      import spark.implicits._

      println("Hello world!")

    } finally {
      spark.close
    }
  }
}
