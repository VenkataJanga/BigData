package com.zeyobron.venkata.spark.may31

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.log4j.Level
import org.apache.log4j.Logger

/******
 * 
 * Task 5 --
		DSL -
    Two files 
    txn_details and prod_details
    /home/cloudera/txns_dir -----  txn_details
    /home/cloudera/prod_dir -----  prod_details
    Filter txn_details category gymnastics and  Team Sports
    Filter prod_details with spendby credit
    Join these two dataframes using DSL
    Write the data to hdfs in the format of XML with roottag and txns and row tag and records
    Test in your local first and then deploy it as a jar without any hardcod
 * 
 * 
 * ****/
object DSL_Assignment_5 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.FATAL)
    println("Hello Sai........")
    val config = new SparkConf().setAppName("DSLAssignment1").setMaster("local[*]")
    val sc = new SparkContext(config)
    sc.setLogLevel("ERROR")
    val sparkSession= SparkSession.builder().appName("DSLAssignment1").master("local[*]").getOrCreate()
    import sparkSession.implicits._
    
    
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val txns_read_file1 = sparkSession.read.format("csv").option("header",true).load(file_path+"txns_details.csv")
    val product_read_file1 = sparkSession.read.format("csv").option("header",true).load(file_path+"product_details.csv")
    txns_read_file1.show(4)
    println
    product_read_file1.show(4)
    
    val dsl_txns_df = txns_read_file1
                            .selectExpr("*")
                            //.where(col("category")==="Gymnastics" || col("category")==="Team Sports")
                            .filter(col("category")==="Gymnastics" || col("category")==="Team Sports")
                             
    dsl_txns_df.show(5)
    
    val dsl_prod_df = product_read_file1.selectExpr("*")//Filter prod_details with spendby credit
                                        .filter(col("spendby")==="credit")
    dsl_prod_df.show(2)
    
    //Join these two dataframes using DSL
    val dsl_join_df = dsl_txns_df.join(dsl_prod_df,dsl_prod_df("txnno")===dsl_txns_df("txnno"), "inner").drop(dsl_prod_df("txnno"))
    dsl_join_df.show(4)
    
   // Write the data to hdfs in the format of XML with roottag and txns and row tag and records
    //first trying in local
    dsl_join_df.write.format("com.databricks.spark.xml")
                     .option("header", true)
                     .mode("overwrite")
                     .option("rootTag", "txns")
                     .option("rowTag", "records")
                     .save(file_path+"//Outputs//DSL_PROD_TXNS_DATA")
    println("Successfully Completed task 5 in local System")
}