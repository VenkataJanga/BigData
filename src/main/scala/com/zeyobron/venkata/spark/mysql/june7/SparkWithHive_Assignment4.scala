package com.zeyobron.venkata.spark.mysql.june7

import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.SparkContext
import scala.io.Source
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.SparkSession
import com.mysql.jdbc.Driver
import org.apache.spark.sql.hive._


object SparkWithHive_Assignment4 extends App {
  
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
  val cloud_url = args(0)
  val test_read_zip_hive = args(1)
  val dbtable_user_zip_codes_mysql = args(2)
  val user_root = args(3)
  val password_cloudera = args(4)
  
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
     user_df.coalesce(1).write.format("csv").option("header", true).mode(mode).save(cloud_url)
  }
 
  println("Reading the data from HDFS ")
  val laod_result = spark.read.format("csv")
                              .option("header", true)
                              .option("inferSchema", true)
                              .load(cloud_url)
  laod_result.show(10) 
  
    println("**********Writing hdfs file into Hive table********************************* ")
     laod_result.write.format("hive").option("header", true).mode(SaveMode.Append).saveAsTable(test_read_zip_hive)
    println("**********Completed********************************* ")
    
   println("**********Writing hdfs file into MYSQL table********************************* ")
   laod_result.write.format("jdbc")
                    .option("url", "jdbc:mysql://localhost/zeyobron_mysql_integration")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .mode(SaveMode.Overwrite)
                    .option("dbtable", dbtable_user_zip_codes_mysql)
                    .option("user", user_root)
                    .option("password", password_cloudera)
                    .option("header", true)
                    .save()
   println("**********reading the data from Hive table********************************* ") 
   
 
   val sql_string = "select count(*) from "+test_read_zip_hive
   spark.sql(sql_string).show()
   
  val sql_Df =  spark.read.format("jdbc")
                    .option("url", "jdbc:mysql://localhost/zeyobron_mysql_integration")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("dbtable", dbtable_user_zip_codes_mysql)
                    .option("user", user_root)
                    .option("password", password_cloudera)
                    .option("header", true)
                    .load()

  
}