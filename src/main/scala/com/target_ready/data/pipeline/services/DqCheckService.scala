package com.target_ready.data.pipeline.services

import org.apache.spark.sql.{DataFrame, SparkSession}
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import com.target_ready.data.pipeline.dqCheck.DqCheckMethods._
import com.target_ready.data.pipeline.services.DbService._
import org.apache.spark.internal.Logging

object DqCheckService extends Logging {

  // Function to execute Data Quality (DQ) checks
  def executeDqCheck()(implicit spark: SparkSession): Unit = {

    /** =========================================== READING DATA FROM MYSQL TABLE =========================================== */
    val dfReadStaged: DataFrame = sqlReader(JDBC_DRIVER, TABLE_NAME, JDBC_URL, USER_NAME, KEY_PASSWORD)(spark)


    /** ================================================ CHECK NULL VALUES ================================================ */
    val dfCheckNull: Boolean = dqNullCheck(dfReadStaged, COLUMNS_CHECK_NULL_DQ_CHECK_CLICKSTREAM_DATA)
    logInfo("Data quality null check completed.")


    /** ============================================= CHECK DUPLICATE VALUES ============================================= */
    val dfCheckDuplicate: Boolean = DqDuplicateCheck(dfReadStaged, COLUMNS_PRIMARY_KEY_CLICKSTREAM, EVENT_TIMESTAMP_OPTION)
    logInfo("Data quality duplicate check completed.")


    /** ========================================== WRITING TO PROD TABLE IN MYSQL ========================================== */
    if (dfCheckNull && dfCheckDuplicate) {
      sqlWriter(dfReadStaged, TABLE_NAME_FINAL, JDBC_URL)
      logInfo("Data write to production table complete.")
    }

  }

}
