package com.zeyobron.venkata.spark.mysql.june7
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.SaveMode
object SparkReadAndWrite_Assignment6 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("SparkWithMySQLIntegration").master("local[*]").getOrCreate()
    println("Hello Sai..........")
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val read_json_df = spark.read.format("json").option("multiLine", true).load(file_path+"batters.json")
    read_json_df.printSchema()
    val batter_df = read_json_df.withColumn("batters", explode(col("batters.batter")))
                .withColumn("topping", explode(col("topping")))
                .select(col("batters.id").alias("batters_id"),
                        col("batters.type").alias("batters_type"),
                        col("topping.id").alias("topping_id"),
                        col("topping.type").alias("topping_type"),
                        col("id"),col("name"),col("ppu"))
    println(s"The batters count is ${batter_df.count()}")
    batter_df.write.format("csv").mode(SaveMode.Overwrite).save(file_path+"Outputs//Batter_Json")
    println("Succssfully Completed..........")
}