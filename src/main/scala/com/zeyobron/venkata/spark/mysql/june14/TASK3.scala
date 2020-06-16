package com.zeyobron.venkata.spark.mysql.june14

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.functions.regexp_replace

object TASK3 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf().setAppName("Spark with RDD").setMaster("local[*]")
    val sparkContext = new SparkContext(config)
    val spark = SparkSession.builder().getOrCreate()
    val file_path = "file:///K://ZEYOBRON//DATA//"
    
    
    val read_data = spark.read.format("csv")
                              .option("sep","~")
                              .option("header","true")
                              .option("inferSchema","true")
                              .load(file_path+"PractitionerLatest.txt")
      read_data.printSchema()
      read_data.show(4)
                              
    val explodedDF= read_data.withColumn("PROV_PH_DTL",split(col("PROV_PH_DTL"),"\\$").cast("array<string>"))
                    .withColumn("PROV_PH_DTL",explode_outer(col("PROV_PH_DTL")))
                    .withColumn("PROV_ADDR_DTL", split(col("PROV_ADDR_DTL"), "\\$").cast("array<string>")) 
                    .withColumn("PROV_ADDR_DTL", explode_outer(col("PROV_ADDR_DTL"))) 
                    .withColumn("PROV_PH_DTL", regexp_replace(col("PROV_PH_DTL"), "^[ \\t]+|[ \\t]+$", ""))
                    .withColumn("PROV_ADDR_DTL", regexp_replace(col("PROV_ADDR_DTL"), "^[ \\t]+|[ \\t]+$", ""))
                    
   println                              
   println                             
   explodedDF.show(5,false)                
  val cleansedDF= explodedDF.withColumn("PROV_PH_DTL",split(col("PROV_PH_DTL"),"\\|"))
                                .withColumn("PROV_PH_DTL1",col("PROV_PH_DTL").getItem(0))
                                .withColumn("PROV_PH_DTL2", col("PROV_PH_DTL").getItem(1))
                                .withColumn("PROV_PH_DTL3", col("PROV_PH_DTL").getItem(2))
                                .withColumn("PROV_PH_DTL4", col("PROV_PH_DTL").getItem(3))
                                .drop(col("PROV_PH_DTL"))
                                .withColumn("PROV_ADDR_DTL", split(col("PROV_ADDR_DTL"), "\\|")) 
                                .withColumn("PROV_ADDR_DTL1", col("PROV_ADDR_DTL").getItem(0)) 
                                .withColumn("PROV_ADDR_DTL2", col("PROV_ADDR_DTL").getItem(1)) 
                                .withColumn("PROV_ADDR_DTL3", col("PROV_ADDR_DTL").getItem(2)) 
                                .withColumn("PROV_ADDR_DTL4", col("PROV_ADDR_DTL").getItem(3))
                                .withColumn("PROV_ADDR_DTL5", col("PROV_ADDR_DTL").getItem(4)) 
                                .withColumn("PROV_ADDR_DTL6", col("PROV_ADDR_DTL").getItem(5)) 
                                .withColumn("PROV_ADDR_DTL7", col("PROV_ADDR_DTL").getItem(6)) 
                                .withColumn("PROV_ADDR_DTL8", col("PROV_ADDR_DTL").getItem(7)) 
                                .withColumn("PROV_ADDR_DTL9", col("PROV_ADDR_DTL").getItem(8)) 
                                .withColumn("PROV_ADDR_DTL10", col("PROV_ADDR_DTL").getItem(9)) 
                                .withColumn("PROV_ADDR_DTL11", col("PROV_ADDR_DTL").getItem(10)) 
                                .withColumn("PROV_ADDR_DTL12", col("PROV_ADDR_DTL").getItem(11)) 
                                .withColumn("PROV_ADDR_DTL13", col("PROV_ADDR_DTL").getItem(12)) 
                                .drop(col("PROV_ADDR_DTL"))

   println                              
   println                             
   cleansedDF.show(5,false)
  
}