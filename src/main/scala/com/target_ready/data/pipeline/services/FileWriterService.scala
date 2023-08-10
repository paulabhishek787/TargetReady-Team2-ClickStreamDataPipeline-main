package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.exceptions.FileWriterException
import com.target_ready.data.pipeline.constants.ApplicationConstants.{CHECKPOINT_LOCATION, SERVER_ID}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.{OutputMode,Trigger}

object FileWriterService {

  /** ===============================================================================================================
   *  FUNCTION TO WRITE DATA INTO KAFKA STREAM
   *
   *  @param df             the dataframe taken as an input
   *  @param topic          kafka topic name
   *  ==============================================================================================================*/
  def writeDataToStream(df: DataFrame, topic: String): Unit = {
    try {
      df
        .selectExpr("CAST(value AS STRING)") // Convert the value column to string for writing to Kafka
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", SERVER_ID)
        .option("topic", topic)
        .save()
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + topic)
    }
  }




  /** ===============================================================================================================
   *  FUNCTIONS TO SAVE DATA INTO OUTPUT LOCATION
   *
   *  @param df             the dataframe taken as an input
   *  @param filePath       the location where null values will be written
   *  @param fileFormat     specifies format of the file
   *  ==============================================================================================================*/
  def writeDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .start()
        .awaitTermination(10000)

    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }




  /** ===============================================================================================================
   *  FUNCTIONS TO SAVE DATA INTO SQL TABLE
   *
   *  @param df          the dataframe taken as an input
   *  @param driver      MySql driver
   *  @param tableName   MySql table name
   *  @param jdbcUrl     jdbc URL
   *  @param user        MySql database username
   *  @param password    MySql database password
   *  ============================================================================================================ */
  def writeDataToSqlServer(df: DataFrame, driver: String, tableName: String, jdbcUrl: String, user: String, password: String): Unit = {
    df.writeStream
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        batchDF.write
          .format("jdbc")
          .option("driver", driver)
          .option("url", jdbcUrl)
          .option("dbtable", tableName)
          .option("user", user)
          .option("password", password)
          .mode("overwrite")
          .save()
      }
      .outputMode(OutputMode.Append())
      .start().awaitTermination(150000)
  }




  /** ===============================================================================================================
   *  FUNCTION TO SAVE NULL-VALUE DATA INTO NULL-VALUE-OUTPUT LOCATION
   *
   *  @param df             the dataframe taken as an input
   *  @param filePath       the location where null values will be written
   *  @param fileFormat     specifies format of the file
   *  ============================================================================================================== */
  def writeNullDataToOutputDir(df: DataFrame, fileFormat: String, filePath: String): Unit = {
    try {
      df.writeStream
        .outputMode("append")
        .format(fileFormat)
        .option("path", filePath)
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .trigger(Trigger.Once()) // Trigger the write operation only once
        .start()
        .awaitTermination(10000)
    } catch {
      case e: Exception => FileWriterException("Unable to write files to the location: " + filePath)
    }
  }
}
