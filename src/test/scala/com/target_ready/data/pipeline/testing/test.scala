package com.target_ready.data.pipeline.testing
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}

object test {
  def main(args: Array[String]): Unit = {
    // Set log level to INFO
    Logger.getRootLogger.setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .appName("LoggingExample")
      .master("local")
      .getOrCreate()
    val logger = Logger.getLogger(getClass)

    import spark.implicits._

    // Sample DataFrame
    val data = Seq(
      (1, "Alice", 25),
      (2, "Bob", 30),
      (3, "John", 28)
    )
    val columns = Seq("id", "name", "age")
    val df = data.toDF(columns: _*)

    // Logging example
    logger.info("Starting the Spark application")

    // Do some data processing here...

    // Show the DataFrame
    df.show()

    logger.info("Data processing completed")

    // Stop the SparkSession when you're done
    spark.stop()
  }
}

