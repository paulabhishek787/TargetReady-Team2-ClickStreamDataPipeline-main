package com.target_ready.data.pipeline.services

import com.target_ready.data.pipeline.services.FileReaderService._
import com.target_ready.data.pipeline.services.FileWriterService._
import org.apache.spark.sql.{DataFrame, SparkSession}
import com.target_ready.data.pipeline.constants.ApplicationConstants._
import com.target_ready.data.pipeline.clenser.Clenser._
import com.target_ready.data.pipeline.transform.JoinTransformation._
import org.apache.spark.internal.Logging

object PipelineService extends Logging {

  def executePipeline()(implicit spark: SparkSession): Unit = {

    /** ==============================================================================================================
     *                            Reading the data from source directory (.csv file)
     *  ============================================================================================================ */
    val ITEM_DATA_DF: DataFrame = readFile(INPUT_FILE_PATH_ITEM_DATA, INPUT_FORMAT_ITEM_DATA)(spark)
    val CLICKSTREAM_DATA_DF: DataFrame = readFile(INPUT_FILE_PATH_CLICKSTREAM_DATA, INPUT_FORMAT_CLICKSTREAM)(spark)


    /** ==============================================================================================================
     *                      Concatenating the data columns into one single columns as value
     *  ============================================================================================================ */
    val CONCATENATED_ITEM_DATA = concatenateColumns(ITEM_DATA_DF, COLUMN_NAMES_ITEM_DATA,VALUE,",")
    val CONCATENATED_CLICKSTREAM_DATA = concatenateColumns(CLICKSTREAM_DATA_DF, COLUMN_NAMES_CLICKSTREAM_DATA,VALUE,",")
    logInfo("Clickstream data read from input location complete.")



    /** ==============================================================================================================
     *                            Sending the dataframe into kafka topic: writeStream
     *  ============================================================================================================ */
    writeDataToStream(CONCATENATED_ITEM_DATA, TOPIC_NAME_ITEM_DATA)
    writeDataToStream(CONCATENATED_CLICKSTREAM_DATA, TOPIC_NAME_CLICKSTREAM_DATA)
    logInfo("Sending the dataframe into kafka topic Complete")



    /** ==============================================================================================================
     *                           Subscribing to the topic and reading data from stream
     *  ============================================================================================================ */
    val LOAD_DF_ITEM_DATA_DF = loadDataFromStream(TOPIC_NAME_ITEM_DATA)(spark)
    val LOAD_CLICKSTREAM_DF = loadDataFromStream(TOPIC_NAME_CLICKSTREAM_DATA)(spark)
    logInfo("Subscribing to the topic and reading data from stream Complete")
    //LOAD_CLICKSTREAM_DF.printSchema()


    /** ==============================================================================================================
     *                        Splitting Dataframe value-column-data into Multiple Columns
     *  ============================================================================================================ */
    val SPLIT_DATA_DF: DataFrame = splitColumns(LOAD_DF_ITEM_DATA_DF,VALUE,",",COLUMN_NAMES_ITEM_DATA)
    val SPLIT_CLICKSTREAM_DATA_DF: DataFrame = splitColumns(LOAD_CLICKSTREAM_DF,VALUE,",",COLUMN_NAMES_CLICKSTREAM_DATA)
    logInfo("Splitting Dataframe value-column-data into Multiple Columns Complete")



    /** ==============================================================================================================
     *                                            Validating Dataframes
     *  ============================================================================================================ */
    val VALIDATED_ITEM_DATA_DF = dataTypeValidation(SPLIT_DATA_DF, COLUMNS_VALID_DATATYPE_ITEM,NEW_DATATYPE_ITEM)
    val VALIDATED_CLICKSTREAM_DF = dataTypeValidation(SPLIT_CLICKSTREAM_DATA_DF, COLUMNS_VALID_DATATYPE_CLICKSTREAM,NEW_DATATYPE_CLICKSTREAM)



    /** ==============================================================================================================
     *                                    Converting SPLIT_DATA_DF to UPPERCASE
     *  ============================================================================================================ */
    val UPPERCASE_DF = uppercaseColumns(VALIDATED_ITEM_DATA_DF)
    val UPPERCASE_CLICKSTREAM_DF = uppercaseColumns(VALIDATED_CLICKSTREAM_DF)
    logInfo("Converting SPLIT_DATA_DF to UPPERCASE Complete")



    /** ==============================================================================================================
     *                                            Trimming UPPERCASE_DF
     *  ============================================================================================================ */
    val TRIMMED_DF = trimColumn(UPPERCASE_DF)
    val TRIMMED_CLICKSTREAAM_DF = trimColumn(UPPERCASE_CLICKSTREAM_DF)
    logInfo("Trimming UPPERCASE_DF Complete")



    /** ==============================================================================================================
     *                                  Removing null value rows from TRIMMED_DF
     *  ============================================================================================================ */
    val REMOVED_NULL_VAL_DF = findRemoveNullKeys(TRIMMED_DF, COLUMNS_CHECK_NULL_DQ_CHECK_ITEM_DATA,NULL_VALUE_PATH,NULL_VALUE_FILE_FORMAT)
    val REMOVED_NULL_VAL_CLICKSTREAM_DF = findRemoveNullKeys(TRIMMED_CLICKSTREAAM_DF, COLUMNS_PRIMARY_KEY_CLICKSTREAM,NULL_VALUE_PATH,NULL_VALUE_FILE_FORMAT)
    logInfo("Removing null value rows from TRIMMED_DF Complete")



    /** ==============================================================================================================
     *                               Removing duplicate rows from REMOVED_NULL_VAL_DF
     *  ============================================================================================================ */
    //    val REMOVED_DUP_VAL_DF = removeDuplicates(REMOVED_NULL_VAL_DF, COLUMNS_PRIMARY_KEY_ITEM_DATA, None)
    //    val REMOVED_DUP_VAL_CLICKSTREAM_DF = removeDuplicates(REMOVED_NULL_VAL_CLICKSTREAM_DF, COLUMNS_PRIMARY_KEY_CLICKSTREAM, None)

    val REMOVED_DUP_VAL_DF = dropDuplicates(REMOVED_NULL_VAL_DF, COLUMNS_PRIMARY_KEY_ITEM_DATA)
    val REMOVED_DUP_VAL_CLICKSTREAM_DF = dropDuplicates(REMOVED_NULL_VAL_CLICKSTREAM_DF, COLUMNS_PRIMARY_KEY_CLICKSTREAM)
    logInfo("Removing duplicate rows from REMOVED_NULL_VAL_DF Complete")



    /** ==============================================================================================================
     *                                 Converting REMOVED_NULL_VAL_DF to LOWERCASE
     *  ============================================================================================================ */
    val LOWERCASE_ITEM_DATA_DF = lowercaseColumns(REMOVED_DUP_VAL_DF)
    val LOWERCASE_CLICKSTREAM_DF = lowercaseColumns(REMOVED_DUP_VAL_CLICKSTREAM_DF)
    logInfo("Converting REMOVED_NULL_VAL_DF to LOWERCASE Complete")



    /** ==============================================================================================================
     *                                                JOINING
     *  ============================================================================================================ */
    val JOINED_DF=joinTable(LOWERCASE_CLICKSTREAM_DF,LOWERCASE_ITEM_DATA_DF,JOIN_KEY,JOIN_TYPE_NAME)
    logInfo("Joining of click-stream data and item data is Complete")



    /** ==============================================================================================================
     *              Saving the final transformed data in output location in required output format(.orc)
     *  ============================================================================================================ */
    writeDataToSqlServer(JOINED_DF, JDBC_DRIVER, TABLE_NAME, JDBC_URL, USER_NAME, KEY_PASSWORD)
    logInfo("Writing data into MySql table complete")
  }
}
