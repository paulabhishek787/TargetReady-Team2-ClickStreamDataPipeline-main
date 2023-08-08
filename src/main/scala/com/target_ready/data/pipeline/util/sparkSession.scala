package com.target_ready.data.pipeline.util

import org.apache.spark.sql.SparkSession
import  com.target_ready.data.pipeline.constants.ApplicationConstants.{APP_NAME,MASTER_SERVER}

object sparkSession {

  /** ==============================================================================================================
   *                                       FUNCTION TO CREATE SPARK SESSION
   *  ============================================================================================================ */
  def createSparkSession(): SparkSession = {
      SparkSession.builder()
        .appName(APP_NAME)
        .master(MASTER_SERVER)
        .getOrCreate()
  }
}
