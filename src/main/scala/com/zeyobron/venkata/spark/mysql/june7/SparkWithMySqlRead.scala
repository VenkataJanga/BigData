package com.zeyobron.venkata.spark.mysql.june7

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SaveMode

object SparkWithMySqlRead extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("SparkWithMySQLIntegration").master("local[*]").getOrCreate()
  import spark.implicits._
  
  val sql_query="select * from zeyobron_mysql_integration.user_zip_codes"
                    
  val  randomized_dsl_df =  spark.read.format("jdbc")
                        .option("url", "jdbc:mysql://localhost:3306/zeyobron_mysql_integration")
                        .option("driver", "com.mysql.jdbc.Driver")
                        .option("dbtable", "user_zip_codes")
                        .option("user", "root")
                        .option("password", "root")
                        .load()
  print(randomized_dsl_df.count())     
  randomized_dsl_df.show(5)
  
   val connectionProperties=new java.util.Properties()
        connectionProperties.put("user","root")
        connectionProperties.put("password","root")
    val url="jdbc:mysql://localhost:3306/zeyobron_mysql_integration"
    val table_name = "zip_codes"
    val randomized_dsl_df1 = spark.read.format("jdbc").jdbc(url, table_name,connectionProperties)
     print(randomized_dsl_df1.count())     
      randomized_dsl_df1.show(5)
}