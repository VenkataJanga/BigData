package com.zeyobron.venkata.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType

case class schema_covid_19(Direction:String,Year:String,Date:String,Weekday:String,Current_Match:String,Country:String,Commodity:String,Transport_Mode:String,Measure:String,Value:String,Cumulative:String)
object Covid_19_In_Spark_Params {
  
  def main(args:Array[String]):Unit={
      println("Hello Sai....")
      val config = new SparkConf().setAppName("Covid_19_In_Spark_Params").setMaster("local[*]")
      val sparkContext = new SparkContext(config)
      sparkContext.setLogLevel("ERROR")
      sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    
      
       val sparkSession = SparkSession.builder().appName("Covid_19_In_Spark_Params").getOrCreate()
       import sparkSession.implicits._
       
       
       val source_file_path = args(0)//file:///K://ZEYOBRON//DATA//covid-19.txt
       val schema_source_file_path = args(1)//"file:///K://ZEYOBRON//DATA//schema_file.txt"  
       
       val write_format_csv = args(2) //csv
       val write_format_parquet = args(3) //parquet
       val write_format_json = args(4) //json
       val write_format_avro = args(5) //avro
       
       
       val write_format_overwrite = args(6)//overwrite
       
       val write_tonnes_export_path = args(7)    //Tonnes_exports_df
       val write_dollar_export_path = args(8)    //Dollar_exports_df
       val write_dollar_import_path = args(9)    //Dollar_imports_df
       val write_dollar_reimport_path = args(10)  //Dollar_reimports
       
       val read_tonnes_export_path = args(11)   //Tonnes_exports_df
       val read_dollar_export_path = args(12)   //Dollar_exports_df
       val read_dollar_import_path = args(13)   //Dollar_imports_df
       val read_dollar_reimport_path = args(14) //Dollar_reimports
        
       val covid_root_tag = args(15)//covid_data
       val covid_row_tag = args(16)//report
       
       val write_json_path = args(17) //JSON format
       val write_xml_path = args(18) //XML format
       
        //1)Read the file as an RDD
        val data = sparkContext.textFile(source_file_path)
        
        //2)Remove the header of the RDD  
        val header = data.first() //extract header
        val covid_data = data.filter(row => row != header)   //filter out header
        
         // 3)Create the case class using the schema file given manually with all strings
         //4)Impose the case class to the RDD and filter Measure= Tonnes
        val schmema_with_covid_data = covid_data.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(f=>f.contains("Tonnes"))
          .map(f=>schema_covid(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
          
             
         //5)Schemardd to Dataframe -----1 st dataframe  -- tonnes dataframe
          val tonns_covid_data = schmema_with_covid_data.toDF()
          tonns_covid_data.show()
          
          //6)Have the header removed rdd again convert that into a row rdd 
          //7)Filter Measure index = $
          val row_covid_data = covid_data.map(x=>x.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")).filter(f=>f.contains("$"))
          .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10)))
          
            //8)Create structtype using schema_file given dynamically --- First Challenge
           val rdd_covid_schema = sparkContext.textFile(schema_source_file_path)
           val schema_covid_list = rdd_covid_schema.flatMap(f=>f.split(",")).collect().toList
           val stuct_scema_df = StructType(schema_covid_list.map(f=>StructField(f,StringType,true)))
           
           //9)Convert that into a dataframe --- 2nd dataframe --  dollar dataframe
           val dollar_df = sparkSession.createDataFrame(row_covid_data, stuct_scema_df)
           println("")
           dollar_df.show()
            println("TESTING0.....Tonnes_exports_df BEFORE.................")
           
           val Tonnes_exports_df =  tonns_covid_data.filter(f=>f(0)=="Exports")
           val Dollar_exports_df =  dollar_df.filter(f=>f(0)=="Exports")
           val Dollar_imports_df =  dollar_df.filter(f=>f(0)=="Imports")
           val Dollar_reimports  =  dollar_df.filter(f=>f(0)=="Reimports")
           println("TESTING0.....Tonnes_exports_df AFTER.................")
           
          println("TESTING0.......................")
           //  12)11)tonnes_exports = tonnes.filter(direction=exports)  - Write csv
          Tonnes_exports_df.coalesce(1).write.format(write_format_csv).option("header", true).mode(write_format_overwrite).save(write_tonnes_export_path)
          
        //  13)dollar_exports = dollar.filter(direction=exports) - Write parquet
          Dollar_exports_df.coalesce(1).write.format(write_format_parquet).option("header", true).mode(write_format_overwrite).save(write_dollar_export_path)
          
        //   14)dollar_imports = dollar.filter(direction=imports) - Write json
          Dollar_imports_df.coalesce(1).write.format(write_format_json).option("header", true).mode(write_format_overwrite).save(write_dollar_import_path)
          
        //  15)dollar_reimports = dollar.filter(direction=reimports) - Write AVRO
         Dollar_reimports.coalesce(1).write.format(write_format_avro).option("header", true).mode(write_format_overwrite).save(write_dollar_reimport_path)
         
         
         // 17)Read all the four datas seamlessly with 4 dataframes
         val tonnes_csv_df = sparkSession.read.format(write_format_csv).option("header", true).option("inferSchema", true)
                                     .load(read_tonnes_export_path)
        
         val  dollars_parquet_df = sparkSession.read.format(write_format_parquet).option("header", true).option("inferSchema", true)
                                     .load(read_dollar_export_path)
         
         val  dollar_json_df = sparkSession.read.format(write_format_json).option("header", true).option("inferSchema", true)
                                     .load(read_dollar_import_path)
        
         val  dollars_avro_df = sparkSession.read.format(write_format_avro).option("header", true).option("inferSchema", true)
                                     .load(read_dollar_reimport_path)
         
                                     
         //18)Do the Union 
         val final_covid_data = tonnes_csv_df.union(dollars_parquet_df).union(dollar_json_df).union(dollars_avro_df)
         
         println
         tonnes_csv_df.show()
         
         println("union count",final_covid_data.count())
         
         println("TESTING1.......................")
         
         
          final_covid_data.createOrReplaceTempView("covid_union_data_table")
         //19)Write the union data into a directory in the format on Json (year/month/date/direction partitionby) 
         val partion_df = sparkSession.sql("select *, substr(Date,7,4) as year1, substr(Date,4,2) as month1, substr(Date,1,2) as day1  from covid_union_data_table")
         
         println
         println("Partition count",partion_df.count())
         println
         
         partion_df.coalesce(1).write.format("json").partitionBy("year1", "month1", "day1","Direction").mode("overwrite").save(write_json_path)
         
        /* partion_df.coalesce(1).write.option("rowTag", covid_row_tag).mode("overwrite").option("rootTag", covid_root_tag)
                    .format("com.databricks.spark.xml").option("header", true).save(write_xml_path)*/
                    
        println("Successfully completed..............")
  }
}