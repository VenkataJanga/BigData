package com.zeyobron.venkata.spark.may31

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.sum
import org.apache.log4j.Level
import org.apache.log4j.Logger

object DSLOperations extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val struct_txns = StructType(
                      StructField("txnno" , StringType,true)::
                         StructField("txndate" , StringType,true)::
                          StructField("value", StringType, true)::
                            StructField("amount" , StringType,true)::
                              StructField("category" , StringType,true)::
                                StructField("product" , StringType,true)::
                                  StructField("city" , StringType,true)::
                                   StructField("state", StringType, false)::
                                    StructField("spendby" , StringType,true)::Nil
                  )
                  
                  

  val spark = SparkSession.builder().appName("TxnsData").master("local[*]").getOrCreate()
  import spark.implicits._
  val txnsdata = spark.read.option("header", false)
                         .schema(struct_txns)
                         .option("inferSchema", true)
                         .format("csv")
                         .load("src/main/resources/txns_data")
                         
  txnsdata.show()
 
  txnsdata.createOrReplaceTempView("txns_table")
  
  val df1 = txnsdata.select("*").show()   //val df1 = spark.sql("select * form txns_table")
  
  val df2 = txnsdata.select("txnno","txndate").show(5) //val df1 = spark.sql("select txnno, txndate form txns_table")
  val df3 = txnsdata.select("*").filter($"category" === "Gymnastics").show() //val df1 = spark.sql("select * form txns_table where category == 'Gymnastics' ")
  val df4 = txnsdata.select("*").filter(col("category") === "Gymnastics").show()//both are same
  val df5 = txnsdata.select("*").filter(col("category") === "Gymnastics" || col("spendby")==="credit").show()//both are same
  
  val df6 = spark.sql(""" select * from txns_table where  category =='Gymnastics'
     or spendby = 'credit' """).show() // """ is used for multiline
  
  val df7 = txnsdata.select("*").filter(col("spendby")==="cash").show()
  
  val df8 = spark.sql(""" select * from txns_table where  category like '%Sports'
     and spendby = 'credit' """).show()
     
  val df9 = txnsdata.select("*").filter(col("category") like "%Sports").show()
  
   val df10 = spark.sql(""" select * from txns_table where  category in('Team Sports','Combat Sports')
      """).show()
   val df11= txnsdata.select("*").filter(col("category") isin("Team Sports","Combat Sports")).show()
   
    val df12 = spark.sql(""" select * from txns_table where  category is NULL""").show()
    val df13= txnsdata.select("txnno","txndate").filter(col("category").isNotNull && col("spendBy").isNotNull).show()
   
    val df14= txnsdata.selectExpr("txnno","txndate as txn_date","substring(txndate,7,4) as year","substring(txndate,4,2) as month","substring(txndate,1,2) as day")
    .filter(col("category").isNotNull && col("spendBy").isNotNull).show()
    
    val df15= txnsdata.selectExpr("txnno","txndate as txn_date","trim(category) as category","substr(txndate,7,4) as year","substr(txndate,4,2) as month","substr(txndate,1,2) as day")
    .filter(col("category").isNotNull && col("spendBy").isNotNull).show()  // df14 and df15 both are same result 
    
    val df101 = spark.sql(""" select txnno, txndate,case when category ='Exercise & Fitness' then 1 else 0 end as code  from txns_table 
               """).show()
               
   val df1011 = txnsdata.selectExpr("txnno","txndate as txn_date","trim(category) as category","substr(txndate,7,4) as year","substr(txndate,4,2) as month","substr(txndate,1,2) as day","case when category ='Exercise & Fitness' then 1 else 0 end as code").show()
    
   val df10111 = txnsdata.withColumn("day", expr("substr(txndate,4,2)")).show()
   
   //txnsdata.selectExpr("txnno","txndate as txn_date","value","amount","split(category,' ') as category[0]","product","city","state","spendby").show()
   
   txnsdata.withColumn("category",expr("split(category,' ')[0]")).show()
   txnsdata.withColumn("category1",expr("split(category,' ')[0]")).show()
   val test =txnsdata.withColumn("category1", expr("split(category,' ')[0]")).withColumn("product", expr("split(product,' ')[0]"))
    test.show()

  val test1 =txnsdata.withColumn("category1", expr("split(category,' ')[0]")).withColumn("product1", expr("split(product,' ')[0]")).withColumn("txn_date", expr("substr(txndate,4,2)"))
  test1.show()
  
  //txnsdata.withColumn("amt", expr("split(amount,\\.)[0]")).show()
  txnsdata.withColumn("amt", split(col("amount"), "\\.").getItem(0)).show()
  
  
  //val df0 = spark.sql(""" txnno,sum(amount) as amount ,category from txns_table group by category""").show()
  
  val test2 =txnsdata.withColumn("category", expr("split(category,' ')[0]")).withColumn("product1", expr("split(product,' ')[0]")).groupBy("category").agg(sum("amount").alias("sum"))
  test2.show()
  
  
  //val joindf = df1.join(df2, df1("")===df2(""),"inner").drop(df1(""))
}


