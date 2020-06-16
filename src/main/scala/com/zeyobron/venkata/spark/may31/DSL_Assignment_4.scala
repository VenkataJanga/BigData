package com.zeyobron.venkata.spark.may31

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr

/****
 * Task 4 -- 
 *  Have two extra columns with year of Date and year of current_match
 *  Write this data to the file system in the format avro with two partitions(2020->2015)
 * 
 * *****/
object DSL_Assignment_4 extends App{
  
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val config = new SparkConf().setAppName("DSLAssignment1").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    val sparkSession= SparkSession.builder().appName("DSLAssignment1").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val covid_read_file = sc.textFile(file_path+"covid-19.txt")
    val covid_data = covid_read_file.filter(row=>row!=covid_read_file.first())// Removing the first row of the data
    
    val rdd_row_covid_data = covid_data
                                      .map(f=>f.split(","))
                                      .filter(f=>f.contains("$"))
                                      .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
        
                                      
     val schema_read_list =  sc.textFile(file_path+"schema_file.txt").flatMap(f=>f.split(",")).collect().toList
     val schema_covid = StructType(schema_read_list.map(row=>StructField(row,StringType,true)))
     
     val covdid_df = sparkSession.createDataFrame(rdd_row_covid_data, schema_covid)
     val dsl_covdid_df = covdid_df.select("*")
                                  .withColumn("Year_Of_Date", expr("substring(Date,7,4)"))
                                  .withColumn("Year_Of_Current_Match", expr("substring(Current_Match,7,4)"))
     dsl_covdid_df.show(3)
                 
     val partition_covid_df = dsl_covdid_df.write.
                                           format("com.databricks.spark.avro")
                                           .partitionBy("Year_Of_Date","Year_Of_Current_Match")
                                           .mode("overwrite")
                                           .save(file_path+"Outputs//partition_covid_data")
                                           
     println("Successfully Completed..............")
}