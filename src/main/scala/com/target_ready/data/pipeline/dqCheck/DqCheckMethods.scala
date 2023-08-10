package com.target_ready.data.pipeline.dqCheck

import org.apache.spark.sql.{Column,DataFrame}
import org.apache.spark.sql.functions._
import com.target_ready.data.pipeline.exceptions.{DqNullCheckException,DqDupCheckException}
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import org.apache.spark.sql.expressions.Window
/** ===============================================================================================================
 * DQ CHECK METHODS
 * Ignore for now (Changes have to be made)
 *  ============================================================================================================== */

// Define a set of Data Quality (DQ) check methods
object DqCheckMethods {

  /** ===============================================================================================================
   * FUNCTION TO CHECK NULL VALUES
   *
   * @param df1           the dataframe1 taken as an input
   * @param keyColumns    column names on which the checks need to be performed
   * @return              true: if no null value is found and vice-versa
   * ============================================================================================================= */
  def dqNullCheck(df: DataFrame, keyColumns: Seq[String]): Boolean = {
    // Create a sequence of Column objects representing the key columns
    val columnNames: Seq[Column] = keyColumns.map(c => col(c))
    
    // Define a condition to check for null values, empty strings, "NULL", and "null"
    val condition: Column = columnNames.map(c => c.isNull || c === "" || c.contains("NULL") || c.contains("null")).reduce(_ || _)
       
    // Add a "nullFlag" column indicating if any of the key columns have null values 
    val dfCheckNullKeyRows: DataFrame = df.withColumn("nullFlag", when(condition, value = true).otherwise(value = false))
    
    // Filter the DataFrame to only include rows with null values
    val nullDf: DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag") === true)

    // If any null values are found, throw a DqNullCheckException
    if (nullDf.count() > 0) throw DqNullCheckException("The file contains nulls")  
    true

  }



  /** ==============================================================================================================
   * FUNCTION TO CHECK DUPLICATES
   *
   * @param df                the dataframe
   * @param KeyColumns        sequence of key columns of the df dataframe
   * @param orderByColumn
   * @return                  true: if no duplicate value is found and vice-versa
   * =============================================================================================================*/
  def DqDuplicateCheck(df: DataFrame, KeyColumns: Seq[String], orderByCol: String): Boolean = {

    // Create a Window specification for partitioning and ordering
    val windowSpec = Window.partitionBy(KeyColumns.map(col): _*).orderBy(desc(orderByCol))

    // Add a row number column to identify duplicates based on ordering
    val dfDropDuplicate: DataFrame = df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
      .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
    
    // Compare the row counts before and after dropping duplicates
    // If counts are not equal, throw a DqDupCheckException
    if (df.count() != dfDropDuplicate.count()) throw DqDupCheckException("The file contains duplicate")
    true
  }
}
