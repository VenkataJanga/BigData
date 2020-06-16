package com.zeyobron.venkata.spark.mysql.june7

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import scala.io.Source
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.SaveMode
object SparkWithMySQL_Assignment2 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("SparkWithMySQLIntegration").enableHiveSupport().master("local[*]").getOrCreate()
    import spark.implicits._
    println("Hello Sai..........")
    val randamUserUrl = "https://randomuser.me/api/0.8/?results=10"
   // var mode ="append"
    
    
    val count = 1 
    
    val prop=new java.util.Properties()    
        prop.put("user","root")
        prop.put("password","root")
       println("test1")  
        
    val url="jdbc:mysql://localhost:3306/zeyobron_mysql_integration"
    
    for(count<-1 to 10){
         val randomized_dsl_df = spark.read.format("json").json(Seq(Source.fromURL(randamUserUrl).mkString).toDS())
         
            //randomized_dsl_df is a dataframe contains the data which you want to write.
             randomized_dsl_df.withColumn("results", explode(col("results")))
                              .select(col("results.user.location.zip").cast(StringType))
                              .write.mode(SaveMode.Append).jdbc(url,"zip_codes1235",prop)
                              print("test")
    }
    println("Succssfully Completed..........")
}