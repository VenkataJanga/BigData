package com.zeyobron.venkata.spark.mysql.june7

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SaveMode
import scala.io.Source
import org.apache.spark.sql.types.StringType

object SparkWithMySQL_Assignment2_Params extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().appName("SparkWithMySQLIntegration").master("local[*]").getOrCreate()
  import spark.implicits._
  //spark.sparkContext.setLogLevel("ERROR")
  println("Hello Sai..........")
  val randamUserUrl = "https://randomuser.me/api/0.8/?results=10"
  val count = 1
  for(count<-1 to 10){
       val randomized_dsl_df = spark.read.format("json").json(Seq(Source.fromURL(randamUserUrl).mkString).toDS())
       randomized_dsl_df.withColumn("results", explode(col("results")))
                        .select(col("results.user.location.zip").cast(StringType))
                        .write.format("jdbc").mode(SaveMode.Append)
                        .option("url", "jdbc:mysql://localhost/zeyobron_mysql_integration")
                        .option("driver", "com.mysql.jdbc.Driver")
                        .option("dbtable", "user_zip_codes_mysql")
                        .option("user", "root")
                        .option("password", "cloudera")
                        .save()
  }
  println("Succssfully Completed..........")
}