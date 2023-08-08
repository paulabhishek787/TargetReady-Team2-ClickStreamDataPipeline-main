package com.target_ready.data.pipeline.clenser

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import com.target_ready.data.pipeline.services.FileWriterService.{writeNullDataToOutputDir,writeDataToOutputDir}
import org.apache.spark.internal.Logging

object Clenser extends Logging {

  /** ==============================================================================================================
   * FUNCTION TO CHANGE THE DATATYPE
   *
   * @param df          the dataframe
   * @param columnNames sequence of columns of the df dataframe
   * @param dataTypes   sequence of data types
   * @return            dataframe with updated data type
   * =============================================================================================================*/
  def dataTypeValidation(df: DataFrame, columnNames: Seq[String], dataTypes: Seq[String]): DataFrame = {
    var dfChangedDataType: DataFrame = df
    for (i <- columnNames.indices) {
      if (dataTypes(i) == TIMESTAMP_DATATYPE)
        dfChangedDataType = dfChangedDataType.withColumn(columnNames(i), unix_timestamp(col(columnNames(i)), TTIMESTAMP_FORMAT).cast(TIMESTAMP_DATATYPE))
      else
        dfChangedDataType = dfChangedDataType.withColumn(columnNames(i), col(columnNames(i)).cast(dataTypes(i)))
    }
    dfChangedDataType
  }




  /** ==============================================================================================================
   * FUNCTION TO FIND AND REMOVE NULL VALUE ROWS FROM DATAFRAME
   *
   * @param df             the dataframe taken as an input
   * @param primaryColumns sequence of primary key columns
   * @param filePath       the location where null values will be written
   * @param fileFormat     specifies format of the file
   * @return notNullDf which is the data free from null values
   * ============================================================================================================ */
  def findRemoveNullKeys(df: DataFrame, primaryColumns: Seq[String], filePath: String, fileFormat: String): DataFrame = {

    val columnNames: Seq[Column] = primaryColumns.map(ex => col(ex))
    val condition: Column = columnNames.map(c => c.isNull || c === "" || c.contains("NULL") || c.contains("null")).reduce(_ || _)
    val dfCheckNullKeyRows: DataFrame = df.withColumn("nullFlag", when(condition, value = true).otherwise(value = false))

    val nullDf: DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag") === true)
    val notNullDf: DataFrame = dfCheckNullKeyRows.filter(dfCheckNullKeyRows("nullFlag") === false).drop("nullFlag")

    writeNullDataToOutputDir(nullDf, fileFormat, filePath)
    print("writeNullDataToOutputDir")

    notNullDf
  }




  /** ==============================================================================================================
   * FUNCTION TO REMOVE DUPLICATE ROWS IN DATAFRAME
   *
   * @param df                the dataframe
   * @param primaryKeyColumns sequence of primary key columns of the df dataframe
   * @param orderByColumn
   * @return dataframe with no duplicates
   *         ============================================================================================================ */
  def removeDuplicates(df: DataFrame, primaryKeyColumns: Seq[String], orderByColumn: Option[String]): DataFrame = {

    val dfDropDuplicates: DataFrame = orderByColumn match {
      case Some(orderCol) =>
        val windowSpec = Window.partitionBy(primaryKeyColumns.map(col): _*).orderBy(desc(orderCol))
        df.withColumn(colName = ROW_NUMBER, row_number().over(windowSpec))
          .filter(col(ROW_NUMBER) === 1).drop(ROW_NUMBER)
      case _ => df.dropDuplicates(primaryKeyColumns)
    }
    dfDropDuplicates

  }

  def dropDuplicates(df: DataFrame, columns: Seq[String]): DataFrame = {
    df.dropDuplicates(columns)
  }



  /** ==============================================================================================================
   *  FUNCTION TO UPPERCASE DATAFRAME COLUMNS
   *
   *  @param df     the dataframe
   *  @return       dataframe with uppercase columns
   *  ============================================================================================================ */
  def uppercaseColumns(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns) resultDf = resultDf.withColumn(colm, upper(col(colm)))
    resultDf
  }




  /** ==============================================================================================================
   *  FUNCTION TO LOWERCASE DATAFRAME COLUMNS
   *
   *  @param df     the dataframe
   *  @return       dataframe with lowercase columns
   *  ============================================================================================================ */
  def lowercaseColumns(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns) resultDf = resultDf.withColumn(colm, lower(col(colm)))
    resultDf
  }




  /** ===============================================================================================================
   *  FUNCTION TO TRIM DATAFRAME COLUMNS
   *
   *  @param df     the dataframe
   *  @return       trimmed dataframe
   *  ============================================================================================================ */
  def trimColumn(df: DataFrame): DataFrame = {
    val columns = df.columns
    var resultDf = df

    for (colm <- columns) {
      resultDf = df.withColumn(colm, trim(col(colm)))
      resultDf = df.withColumn(colm, ltrim(col(colm)))
      resultDf = df.withColumn(colm, rtrim(col(colm)))
    }
    resultDf
  }




  /** ==============================================================================================================
   *  FUNCTION TO SPLIT READ-STREAM DATAFRAME COLUMN(Value) TO MULTIPLE COLUMNS
   *
   * @param df                          the dataframe taken as an input
   * @param ConcatenatedColumnName      column name which needs to be split
   * @param separator                   data separator(,)
   * @param originalColumnNames         column names for new dataframe
   * @return                            return dataframe with original column names
   *  ============================================================================================================ */
  def splitColumns(df: DataFrame, ConcatenatedColumnName: String, separator: String, originalColumnNames: Seq[String]): DataFrame = {
    val splitCols = originalColumnNames.zipWithIndex.map { case (colName, index) =>
      split(col(ConcatenatedColumnName), separator).getItem(index).alias(colName)
    }
    df.select(splitCols: _*)
  }




  /** ==============================================================================================================
   *  FUNCTION TO CONCATENATE READ-STREAM DATAFRAME COLUMN(Value) TO MULTIPLE COLUMNS
   *
   *  @param df             the dataframe taken as an input
   *  @param columnNames    column names of given dataframe
   *  @param newColumnName  Concatenated column name
   *  @param separator      data separator
   *  @return               return concatenated dataframe
   *  =========================================================================================================== */
  def concatenateColumns(df: DataFrame, columnNames: Seq[String],newColumnName:String,separator:String): DataFrame = {

    df.withColumn(newColumnName, concat_ws(separator, columnNames.map(col): _*))

  }

}
