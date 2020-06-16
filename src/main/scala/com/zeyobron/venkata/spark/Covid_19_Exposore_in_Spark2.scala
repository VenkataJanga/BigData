package com.zeyobron.venkata.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType



case class schema_covid1(
    Direction:String,
    Year:String,
    Date:String,
    Weekday:String,
    Current_Match:String,
    Country:String,
    Commodity:String,
    Transport_Mode:String,
    Measure:String,
    Value:String,
    Cumulative:String)
object Covid_19_Exposore_in_Spark2 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf().setAppName("SparkSql with RDD").setMaster("local[*]")
    val sparkContext = new SparkContext(config)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    
      //1)Read the file as an RDD
    // val data = sparkContext.textFile("file:///K://ZEYOBRON//DATA//covid-19.txt")
     var  source_path = args(0)
     val data = sparkContext.textFile(source_path)
   
      data.take(5).foreach(println)
      
      println("TESTING.....................")
}