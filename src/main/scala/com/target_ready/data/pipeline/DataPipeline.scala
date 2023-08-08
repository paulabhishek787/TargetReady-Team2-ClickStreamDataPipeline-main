package com.target_ready.data.pipeline

import com.target_ready.data.pipeline.constants.ApplicationConstants
import com.target_ready.data.pipeline.exceptions._
import com.target_ready.data.pipeline.services.PipelineService
import com.target_ready.data.pipeline.services.DqCheckService
import com.target_ready.data.pipeline.util.sparkSession.createSparkSession
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object DataPipeline extends Logging {

  var exitCode: Int = ApplicationConstants.FAILURE_EXIT_CODE

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

      PipelineService.executePipeline()(spark)
      logInfo("Executing Pipeline complete.")
      DqCheckService.executeDqCheck()(spark)
      logInfo("Executing DqChecks complete.")

    } catch {
      case ex: FileReaderException =>
        logError("File read exception", ex)

      case ex: FileWriterException =>
        logError("file write exception", ex)

      case ex: DqNullCheckException =>
        logError("DQ check failed", ex)

      case ex: DqDupCheckException =>
        logError("DQ check failed", ex)

      case ex: Exception =>
        logError("Unknown exception", ex)
    }
    finally {
      logInfo(s"Pipeline completed with status $exitCode")
      spark.stop()
      sys.exit(exitCode)
    }

  }
}
