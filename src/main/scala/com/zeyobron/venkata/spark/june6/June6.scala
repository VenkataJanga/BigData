package com.zeyobron.venkata.spark.june6

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.col

object June6 extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val sparkSession= SparkSession.builder().appName("DSL_Assignment_1").master("local[*]").getOrCreate()
    import sparkSession.implicits._
     
    val file_path = "file:///K://ZEYOBRON//DATA//"
    //val person_data = sparkSession.read.format("json").option("header", true).load(file_path+"person_details.json")
    
    /***********
     * 
     * the below code is works in prior to SPark2.0 version
     * */
    
     val person_data1 = sparkSession.read.format("json").option("multiLine", true).load(file_path+"person_details.json")
     person_data1.printSchema()
     person_data1.show(false)
     person_data1.select("Place", "Native","Nationality","Employee_id","Employee_name","Employee_client").show()

     
     val person_data2 = sparkSession.read.format("json").option("multiLine", true).load(file_path+"person_details1.json")
     person_data2.printSchema()
     person_data2.show(false)
     person_data2.select("Place", "Native","Nationality","Employee.id","Employee.name","Employee.client").show()
     
      val person_data3 = sparkSession.read.format("json").option("multiLine", true).load(file_path+"person_details2.json")
     person_data3.printSchema()
     person_data3.show(false)
     person_data3.select("Place", "Native","Nationality","Employee1.id","Employee2.id","Employee1.name","Employee1.client").show()
     
       
      val person_data4 = sparkSession.read.format("json").option("multiLine", true).load(file_path+"person_details3.json")
     person_data4.printSchema()
     person_data4.show(false)
     
    person_data4.select($"Employees",explode($"Employees")).show()
      
    person_data4.withColumn("Employees",explode($"Employees"))
					.select("Employees.*","Nationality","Native","Place").show()
					
					   
    person_data4.withColumn("Employees",explode(col("Employees")))
					.select("Employees.*").show()
					
					
					 /* val formated_data = renders_data.withColumn("results", explode(col("results")))
                .select("results.user.gender",
                          "results.user.name.title","results.user.name.first","results.user.name.last",
                          "results.user.location.street","results.user.location.city","results.user.location.state","results.user.location.zip",
                          "results.user.email","results.user.username","results.user.password","results.user.salt","results.user.md5","results.user.sha1",
                          "results.user.sha256","results.user.registered","results.user.dob","results.user.phone","results.user.cell",
                          "results.user.picture.large","results.user.picture.medium","results.user.picture.thumbnail",
                          "nationality","seed","version")*/
}