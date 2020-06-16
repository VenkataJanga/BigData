package com.zeyobron.venkata.spark.mysql.june7

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import org.apache.spark.sql.SaveMode
import org.apache.log4j.Level
import org.apache.spark.sql.types.StringType
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hive._
import com.mysql.jdbc.Driver

object SparkWithHive_Assignment3 extends App{
  
  val conf = new SparkConf().setAppName("SparkWithHive_Assignment3").setMaster("local[*]")
  val sc = new SparkContext(conf)
  sc.setLogLevel("ERROR")
  sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
  
  val spark = SparkSession.builder().enableHiveSupport().config("dfs.client.read.shortcircuit.skip.checksum",true).getOrCreate()
  import spark.implicits._
  val hc    = new HiveContext(sc)
  val randamUserUrl = "https://randomuser.me/api/0.8/?results=10"
  val count =1;
  var mode = ""
  
  for(count<- 1 to 10){
    val http = Source.fromURL(randamUserUrl).mkString
    val result = sc.parallelize(List(http))
    val user_df = spark.read.format("csv").json(result)
                       .withColumn("results", explode(col("results")))
                       .select(col("results.user.location.zip").cast(StringType))
       if(count==1){
         mode = "overwrite"
       }else{
         mode = "append"
       }
     user_df.coalesce(1).write.format("csv").option("header", true).mode(mode).save("hdfs:/user/cloudera/inputdata/read_zip_csv")
  }
 
  println("Reading the data from HDFS ")
  val laod_result = spark.read.format("csv")
                              .option("header", true)
                              .option("inferSchema", true)
                              .load("hdfs:/user/cloudera/inputdata/read_zip_csv")
  laod_result.show(10) 
  
    println("**********Writing hdfs file into Hive table********************************* ")
     laod_result.write.format("hive").option("header", true).mode(SaveMode.Append).saveAsTable("test.read_zip_hive")
    println("**********Completed********************************* ")
    println("**********reading the data from Hive table********************************* ")               
    spark.sql("select count(*) from test.read_zip_hive").show()
   
    
   println("**********Writing hdfs file into MYSQL table********************************* ")
   laod_result.write.format("jdbc")
                    .option("url", "jdbc:mysql://localhost/zeyobron_mysql_integration")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .mode(SaveMode.Overwrite)
                    .option("dbtable", "user_zip_codes_mysql")
                    .option("user", "root")
                    .option("password", "cloudera")
                    .option("header", true)
                    .save()
   
   
  val sql_Df =  spark.read.format("jdbc")
                    .option("url", "jdbc:mysql://localhost/zeyobron_mysql_integration")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("dbtable", "user_zip_codes_mysql")
                    .option("user", "root")
                    .option("password", "cloudera")
                    .option("header", true)
                    .load()
}