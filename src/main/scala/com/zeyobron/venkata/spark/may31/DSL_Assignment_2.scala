package com.zeyobron.venkata.spark.may31

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.sum



object DSL_Assignment_2 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val config = new SparkConf().setAppName("DSL_Assignment_2").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    val sparkSession= SparkSession.builder().appName("DSL_Assignment_2").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val read_covid_data = sc.textFile(file_path+"covid-19.txt")
    val covid_data = read_covid_data.filter(row=>row!=read_covid_data.first())
    val covid_data_df = covid_data.map(f=>f.split(",")).filter(f=>f.contains("$"))
                        .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
    
    val read_schema_list = sc.textFile(file_path+"schema_file.txt").flatMap(f=>f.split(",")).collect().toList
    val covid_schema = StructType(read_schema_list.map(row=>StructField(row, StringType,true)))
    val covid_df = sparkSession.createDataFrame(covid_data_df, covid_schema)
    
    
    val table_name = covid_df.createOrReplaceTempView("covid_table")
    val spark_agg = sparkSession.sql("""select Direction, Weekday,Year,sum(Value) as Total_Sum 
                                        from covid_table 
                                        group by Direction,Weekday,Year, Value""")
    println(" Time taken for SPARK query")
    sparkSession.time(spark_agg.show())
     
     
     val partition_df = covid_df.select("*")
                                      .filter(col("Direction")==="Exports")
                                      .filter(col("Weekday")==="Saturday")
                                      .filter(col("Year")==="2015")
                                      .groupBy(col("Direction"),col("Weekday"),col("Value"),col("Year"))
                                      .agg(sum("Value").alias("Total_Sum"))
    
    println(" Time taken for DSQL query")
    sparkSession.time(partition_df.show())// DSL query has taking less time
}