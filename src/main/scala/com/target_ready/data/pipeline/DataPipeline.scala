package com.target_ready.data.pipeline

// Import necessary libraries and classes

import com.target_ready.data.pipeline.constants.ApplicationConstants
import com.target_ready.data.pipeline.exceptions._
import com.target_ready.data.pipeline.services.PipelineService
import com.target_ready.data.pipeline.services.DqCheckService
import com.target_ready.data.pipeline.util.sparkSession.createSparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

// The main entry point of the data pipeline application
object DataPipeline extends Logging {

// Initialize the exit code to indicate a failure by default
  var exitCode: Int = ApplicationConstants.FAILURE_EXIT_CODE

  // Main function, where the application execution starts
  def main(args: Array[String]): Unit = {

    /** ==============================================================================================================
     *                                            Creating Spark Session
     *  ============================================================================================================ */
    val spark: SparkSession = createSparkSession()
    logInfo("Creating Spark Session complete.")



    /** ==============================================================================================================
     *                                              Executing Pipeline
     *  ============================================================================================================ */
    try {
      // Attempt to execute the data pipeline
      PipelineService.executePipeline()(spark)
      logInfo("Executing Pipeline complete.")
            
      // Execute data quality checks using the DqCheckService
      DqCheckService.executeDqCheck()(spark)
      logInfo("Executing DqChecks complete.")

    } catch {
      case ex: FileReaderException =>
        logError("File read exception", ex) // Log and handle specific exceptions related to file reading

      case ex: FileWriterException =>
        logError("file write exception", ex) // Log and handle specific exceptions related to file writing

      case ex: DqNullCheckException =>
        logError("DQ check failed", ex) // Log and handle specific exceptions related to data quality null checks

      case ex: DqDupCheckException =>
        logError("DQ check failed", ex) // Log and handle specific exceptions related to data quality duplicate checks

      case ex: Exception =>
        logError("Unknown exception", ex) // Log and handle other unexpected exceptions
    }
    finally {      
      // Regardless of success or failure, log the completion status and stop the SparkSession
      logInfo(s"Pipeline completed with status $exitCode")
      spark.stop()
      sys.exit(exitCode) // Exit the application with the exit code
    }

  }
}
