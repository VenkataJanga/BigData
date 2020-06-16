package com.zeyobron.venkata.spark.june6

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.log4j.Level
import org.apache.log4j.Logger

object RendersComplexProcessing extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val sparkSession= SparkSession.builder().appName("DSL_Assignment_1").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val renders_data = sparkSession.read.format("json").option("multiLine", true).load(file_path+"renders.json")
    renders_data.printSchema()
    val formated_data = renders_data.withColumn("results", explode(col("results")))
                .select("results.user.gender",
                          "results.user.name.title","results.user.name.first","results.user.name.last",
                          "results.user.location.street","results.user.location.city","results.user.location.state","results.user.location.zip",
                          "results.user.email","results.user.username","results.user.password","results.user.salt","results.user.md5","results.user.sha1",
                          "results.user.sha256","results.user.registered","results.user.dob","results.user.phone","results.user.cell",
                          "results.user.picture.large","results.user.picture.medium","results.user.picture.thumbnail",
                          "nationality","seed","version")
     formated_data.show(false)      
     formated_data.coalesce(1).write.format("com.databricks.spark.avro").mode("overwrite").option("header", true)
       .save(file_path+"Outputs//Randmised_Data_Avro")
     formated_data.coalesce(1).write.format("csv").mode("overwrite").option("header", true).save(file_path+"Outputs//Randmised_Data_CSV")
     println("Succssfully done..............")
}