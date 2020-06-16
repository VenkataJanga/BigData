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



case class schema_covid(
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
object Covid_19_Exposore_in_Spark extends App {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf().setAppName("SparkSql with RDD").setMaster("local[*]")
    val sparkContext = new SparkContext(config)
    val sparkSession = SparkSession.builder().getOrCreate()
    import sparkSession.implicits._
    
      //1)Read the file as an RDD
     val data = sparkContext.textFile("file:///K://ZEYOBRON//DATA//covid-19.txt")
   // var  source_path = args(0)
    // val data = sparkContext.textFile(source_path)
   
      //2)Remove the header of the RDD  
      val header = data.first() //extract header
      val covid_data = data.filter(row => row != header)   //filter out header
     
    // covid_data.take(3).foreach(println)
     
      // 3)Create the case class using the schema file given manually with all strings
     //4)Impose the case class to the RDD and filter Measure= Tonnes
      val schmema_with_covid_data = covid_data.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(f=>f.contains("Tonnes"))
          .map(f=>schema_covid(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
          
      //5)Schemardd to Dataframe -----1 st dataframe  -- tonnes dataframe
      val tonns_covid_data = schmema_with_covid_data.toDF()
          
     // tonns_covid_data.show(4)
     
       
     //6)Have the header removed rdd again convert that into a row rdd 
      //7)Filter Measure index = $
        val row_covid_data = covid_data.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(f=>f.contains("$"))
          .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
          
          val strcut_covid_data = StructType(
                                    StructField("Direction", StringType,true)::
                                     StructField("Year", StringType,true)::
                                      StructField("Date", StringType,true)::
                                       StructField("Weekday", StringType,true):: 
                                        StructField("Current_Match", StringType,true)::
                                         StructField("Country", StringType,true)::
                                          StructField("Commodity", StringType,true)::
                                           StructField("Transport_Mode", StringType,true)::
                                            StructField("Measure", StringType,true)::
                                             StructField("Value", StringType,true)::
                                              StructField("Value", StringType,true)::Nil
                                  )
         val struct_df_covid = sparkSession.createDataFrame(row_covid_data, strcut_covid_data)
         //struct_df_covid.show(4)  
         
        
         //8)Create structtype using schema_file given dynamically --- First Challenge
         val rdd_covid_schema = sparkContext.textFile("file:///K://ZEYOBRON//DATA//schema_file.txt")
         val schema_covid_list = rdd_covid_schema.flatMap(f=>f.split(",")).collect().toList
         println(s"displaying the second field ${schema_covid_list(2)}")
       
         
         val rdd_covid_schema_data = rdd_covid_schema.flatMap(f=>f.split(",")).collect().toList
         val stuct_scema_df = StructType(rdd_covid_schema_data.map(f=>StructField(f,StringType,true)))
         
         
          //9)Convert that into a dataframe --- 2nd dataframe --  dollar dataframe
         val dollar_df2 = sparkSession.createDataFrame(row_covid_data, stuct_scema_df)
        // dollar_df2.show(4)
         
         
         //10)4 New dataframes
         
        // tonns_covid_data.createOrReplaceTempView("tonns_covid_table")
         // val Tonnes_exports_df =  sparkSession.sql("select * from tonns_covid_table where Direction = 'Exports'")
          val Tonnes_exports_df =  tonns_covid_data.filter(f=>f(0)=="Exports")
          val Dollar_exports_df =  dollar_df2.filter($"Direction"==="Exports")
          val Dollar_imports_df =  dollar_df2.filter($"Direction"==="Imports")
          val Dollar_reimports  =  dollar_df2.filter($"Direction"==="Reimports")
         
          Tonnes_exports_df.show(6)
          /*Dollar_exports_df.show(2)
          Dollar_imports_df.show(2)
          Dollar_reimports.show(2)*/
          
        //  12)11)tonnes_exports = tonnes.filter(direction=exports)  - Write csv
          Tonnes_exports_df.coalesce(1).write.format("csv").option("header", true).mode("overwrite").save("file:///K://ZEYOBRON//Outputs//Tonnes_exports")
          
        //  13)dollar_exports = dollar.filter(direction=exports) - Write parquet
          Dollar_exports_df.coalesce(1).write.format("parquet").option("header", true).mode("overwrite").save("file:///K://ZEYOBRON//Outputs//Dollar_exports")
          
        //   14)dollar_imports = dollar.filter(direction=imports) - Write json
          Dollar_imports_df.coalesce(1).write.format("json").option("header", true).mode("overwrite").save("file:///K://ZEYOBRON//Outputs//Dollar_imports")
          
        //  15)dollar_reimports = dollar.filter(direction=reimports) - Write AVRO
         Dollar_reimports.coalesce(1).write.format("com.databricks.spark.avro").option("header", true).mode("overwrite").save("file:///K://ZEYOBRON//Outputs//Dollar_reimports")
         
        //  16)There should not be crc and success
          sparkSession.conf.set("spark.hadoop.mapred.output.committer.class","com.appsflyer.spark.DirectOutputCommitter")
          sparkSession.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
                   
        // 17)Read all the four datas seamlessly with 4 dataframes
         val tonnes_csv_df = sparkSession.read.format("csv").option("header", true).option("inferSchema", true)
                                     .load("file:///K://ZEYOBRON//Outputs//Tonnes_exports")
        
         val  dollars_parquet_df = sparkSession.read.format("parquet").option("header", true).option("inferSchema", true)
                                     .load("file:///K://ZEYOBRON//Outputs//Dollar_exports")
         
         val  dollar_json_df = sparkSession.read.format("json").option("header", true).option("inferSchema", true)
                                     .load("file:///K://ZEYOBRON//Outputs//Dollar_imports")
        
         val  dollars_avro_df = sparkSession.read.format("com.databricks.spark.avro").option("header", true).option("inferSchema", true)
                                     .load("file:///K://ZEYOBRON//Outputs//Dollar_reimports")
        //18)Do the Union 
         val final_covid_data = tonnes_csv_df.union(dollars_parquet_df).union(dollar_json_df).union(dollars_avro_df)
         
        // tonnes_csv_df.show(3)
         
         
         final_covid_data.createOrReplaceTempView("covid_union_data_table")
         //19)Write the union data into a directory in the format on Json (year/month/date/direction partitionby) 
        /* val covdid_sql = sparkSession.
            sql("select *, from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'yyyy') as year1,from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'dd') as date1,from_unixtime(unix_timestamp(Date,'dd/MM/yyyy'),'MM') as month1 from covid_union_data_table ")
          
          covdid_sql.show(2)*/
         
         val partion_df = sparkSession.sql("select *, substr(Date,7,4) as year1, substr(Date,4,2) as month1, substr(Date,1,2) as day1,Direction as direction  from covid_union_data_table")
         
         
         println
         
        // partion_df.show(5)
         println
         
         
          /*//Write the data in JSON format
         partion_df.coalesce(1).write.partitionBy("year1", "month1", "date1","Direction")
                             .format("json").option("header", true).mode("overwrite")
                             .save("file:///K://ZEYOBRON//Outputs//final_covid_in_JSON__data")
                             
         //Write the data in XML format
          partion_df.coalesce(1).write.option("rowTag", "covid_data").mode("overwrite").option("rootTag", "report")
                    .format("xml").option("header", true).save("file:///K://ZEYOBRON//Outputs//final_covid_in_XML_data")*/   
         
         partion_df.coalesce(1).write.format("json").partitionBy("year1", "month1", "day1","direction").mode("overwrite").save("file:///K://ZEYOBRON//Outputs//final_covid_in_JSON__data")
         partion_df.coalesce(1).write.option("rowTag", "covid_data").mode("overwrite").option("rootTag", "report")
                    .format("xml").option("header", true).save("file:///K://ZEYOBRON//Outputs//final_covid_in_XML_data")
                    
        partion_df.printSchema()
                    
         //20)Write the same union data in the format of XML with Root tag as covid_data,row tag as report  -- 2nd Challenge
         val xml_union_covid_df = sparkSession.read.option("rootTag", "report").option("rowTag", "covid_data").option("header", true)
                       .format("xml").load("file:///K://ZEYOBRON//Outputs//final_covid_in_XML_data")
         xml_union_covid_df.show(2)     
         xml_union_covid_df.printSchema()

}