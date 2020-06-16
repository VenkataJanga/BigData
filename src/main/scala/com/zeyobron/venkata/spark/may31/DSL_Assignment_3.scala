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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.date_format
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.datediff
import org.apache.spark.sql.functions.to_date

object DSL_Assignment_3 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val config = new SparkConf().setAppName("DSL_Assignment_3").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    val sparkSession= SparkSession.builder().appName("DSL_Assignment_3").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val covid_data = sc.textFile(file_path+"covid-19.txt")
    covid_data.take(4).foreach(println)
    val covid_data_df = covid_data.filter(row=>row!=covid_data.first())//removing the first line
    
    val row_covid_data = covid_data_df.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(f=>f.contains("$"))
          .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
          
          
    val schema_list = sc.textFile(file_path+"schema_file.txt").flatMap(row=>row.split(",")).collect().toList
    val struct_schema = StructType(schema_list.map(row => StructField(row,StringType,true)))
    
    val covid_data_with_schema_df = sparkSession.createDataFrame(row_covid_data, struct_schema)
    
    val dsl_covid_df = covid_data_with_schema_df.select("*")
                        .withColumn("Today_Date", date_format(to_date(col("Date"),"dd/MM/yyyy"), "yyyy-MM-dd"))
                        .withColumn("Match_Date", date_format(to_date(col("Current_Match"),"dd/MM/yyyy"), "yyyy-MM-dd"))
                        .withColumn("Number_Of_Days", expr("datediff(Match_Date,Today_Date)"))
    dsl_covid_df.show(5)
     
}