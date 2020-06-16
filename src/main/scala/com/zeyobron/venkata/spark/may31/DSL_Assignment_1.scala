package com.zeyobron.venkata.spark.may31


import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.monotonically_increasing_id

/****
 * 
 * Task 1 --
Take US_data.csv limit 500
Take txns limit 500
usdf(0,1,2,3,4,5,6-500)(id)
txns(0,1,2,3,4,5,6-500)(id) MII,RN
Join these two dataframes Drop txns(id)
Write the data the format on Json in windows
 * 
 * ****/
object DSL_Assignment_1 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val config = new SparkConf().setAppName("DSL_Assignment_1").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    val sparkSession= SparkSession.builder().appName("DSL_Assignment_1").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val txns_data = sc.textFile(file_path+"txns_data")
    val us_data = sc.textFile(file_path+"usdata.csv")
    // Removing the header  
    val header = us_data.first()
    println
    val us_df = us_data.filter(row=>row!=header)
    us_df.take(4).foreach(println)
    
    val struct_schema = StructType(
                          StructField("first_name", StringType,true)::
                            StructField("last_name", StringType,true)::
                              StructField("company_name", StringType,true)::
                                StructField("address", StringType,true)::
                                  StructField("city", StringType,true)::
                                    StructField("state", StringType,true)::
                                      StructField("zip", StringType,true)::
                                        StructField("age", StringType,true)::
                                          StructField("phone1", StringType,true)::
                                            StructField("phone2", StringType,true)::
                                              StructField("email", StringType,true)::
                                                StructField("web", StringType,true):: Nil
                                    ) 
                          
     val rdd_data = us_df.map(f=>f.split(",")).map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10),f(11),f(12)))
     val us_data_df = sparkSession.createDataFrame(rdd_data, struct_schema).limit(500)
     println(s"US DATA COUNT ${us_data_df.count()}")
     
     val dsl_us_df = us_data_df.select("*")
     val dsl_us_column_df = dsl_us_df.withColumn("ID", monotonically_increasing_id)
     
     println("***********************Display DSL US DATA***********************")
     dsl_us_column_df.show(4)
     
     val txns_struct = StructType(
                           StructField("Direction",StringType,true)::
                            StructField("Year",StringType,true)::
                             StructField("Date",StringType,true)::
                              StructField("Weekday",StringType,true)::
                               StructField("Current_Match",StringType,true)::
                                StructField("Country",StringType,true)::
                                 StructField("Commodity",StringType,true)::
                                  StructField("Transport_Mode",StringType,true)::
                                   StructField("Measure",StringType,true)::Nil
                           )
     val tsnx_data = sparkSession.read.format("csv").schema(txns_struct).option("header", true).load(file_path+"txns_data").limit(500)
     val dsl_txns_df = tsnx_data.select("*")
     val dsl_txns_ccolumn_df = dsl_txns_df.withColumn("ID", monotonically_increasing_id)
     println("***********************Display DSL US DATA***********************")
     dsl_txns_ccolumn_df.show(4)
     
     val us_txns_data_df = dsl_us_column_df.join(dsl_txns_ccolumn_df, dsl_txns_ccolumn_df("ID")===dsl_us_column_df("ID"),"inner").drop(dsl_txns_ccolumn_df("ID"))
     println("***********************Display DSL US AND TXNS DATA***********************")
     us_txns_data_df.show(4)
     
     us_txns_data_df.write.format("json").option("header", true).mode("overwrite").save(file_path+"us_txns_data")
    
}