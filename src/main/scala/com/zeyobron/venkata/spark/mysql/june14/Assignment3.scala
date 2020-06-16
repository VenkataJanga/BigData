package com.zeyobron.venkata.spark.mysql.june14

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.functions.explode_outer
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions.trim
import org.apache.spark.sql.Row
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.ArrayType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField

object Assignment3 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("org").setLevel(Level.ERROR)
    val config = new SparkConf().setAppName("Spark with RDD").setMaster("local[*]")
    val sparkContext = new SparkContext(config)
    val spark = SparkSession.builder().getOrCreate()
    val file_path = "file:///K://ZEYOBRON//DATA//"
    val read_data = sparkContext.textFile(file_path+"PractitionerLatest.txt")
    read_data.take(5).foreach(println)
    
    val first_row = read_data.first.split("~").toList
    println()
    
    val read_data1 = read_data.filter(f=>f!=read_data.first)
    read_data1.take(3).foreach(println)
    val data = read_data1.map(x=>x.split("~"))
          .map(f=>Row(f(0),f(1),f(2),f(3),f(4),f(5),f(6),f(7),f(8),f(9),f(10),
                      f(11),f(12),f(13),f(14),f(15),f(16),f(17),f(18),f(19),f(20),
                      f(21),f(22),f(23),f(24),f(25),f(26),f(27),f(28),f(29),f(30),
                      f(31),f(32),f(33),f(34),f(35),f(36),f(37),f(38),f(39),f(40),
                      f(41),f(42)
              ))
              
    val strcut_schema = StructType(
                        StructField("PROV_ID", StringType,true)::
                         StructField("ETL_HASH_VAL", StringType,true)::
                          StructField("ETL_LOAD_DT_TM", StringType,true)::
                           StructField("ETL_LAST_UPDT_DT_TM", StringType,true):: 
                            StructField("ETL_IS_CURR_IND", StringType,true)::
                             StructField("ETL_IS_DEL_IND", StringType,true)::
                              StructField("ETL_RCD_END_DT", StringType,true)::
                               StructField("ETL_RCD_START_DT", StringType,true)::
                                StructField("PROV_NPI", StringType,true)::
                                 StructField("SHARE_NPI", StringType,true)::
                                  StructField("PROV_NM", StringType,true)::
                                   StructField("FRST_NM", StringType,true)::
                                    StructField("MID_NM", StringType,true)::
                                     StructField("LAST_NM", StringType,true):: 
                                      StructField("GENDR_CD", StringType,true)::
                                       StructField("PROV_TY", StringType,true)::
                                        StructField("PROV_STAT", StringType,true)::
                                         StructField("PROV_TIN", StringType,true)::
                                          StructField("PROV_WATCH", StringType,true)::
                                           StructField("MEDCR_PARTCPT", StringType,true)::
                                            StructField("MEDCD_PARTCPT", StringType,true)::
                                             StructField("DEA_CERTF", StringType,true)::
                                              StructField("NT_CD", StringType,true)::
                                               StructField("BILL_ENTTY_IND", StringType,true)::
                                                StructField("UPIN_ID", StringType,true):: 
                                                 StructField("PHRM_CHAIN_ID", StringType,true)::
                                                  StructField("ACCPT_NEW_PATNT_IND", StringType,true)::
                                                   StructField("PROV_CLSFTN_CD", StringType,true)::
                                                    StructField("DEA_NUM", StringType,true)::
                                                     StructField("DT_OF_BRTH", StringType,true)::
                                                      StructField("EMAIL_ADDR", StringType,true)::
                                                       StructField("PROV_SPECLTY_DTL", StringType,true)::
                                                        StructField("PROV_PH_DTL", StringType,true)::
                                                         StructField("PROV_ADDR_DTL", StringType,true)::
                                                          StructField("PROV_INCEN_DTL", StringType,true):: 
                                                           StructField("PROV_TAXNMY_DTL", StringType,true)::
                                                            StructField("PROV_LANG_DTL", StringType,true)::
                                                             StructField("PROV_QUALN_DTL", StringType,true)::
                                                              StructField("SRC_DATA_KEY", StringType,true)::
                                                               StructField("SRC_DATA_DESC", StringType,true)::
                                                                StructField("TENANT_CD", StringType,true)::
                                                                 StructField("PFX", StringType,true)::
                                                                  StructField("OPRN_CD", StringType,true)::Nil
                                )  
      val df = spark.createDataFrame(data, strcut_schema)
          df.show(5,false)   
          
      
     println()
     println()
     println("extract the PROV_PH_DTL and PROV_ADDR_DTL columns with delimiter $ ")
     
     val df1 =  df.withColumn("PROV_PH_DTL",split(col("PROV_PH_DTL"),"\\$").cast(ArrayType(StringType)))
                  .withColumn("PROV_PH_DTL",explode_outer(col("PROV_PH_DTL")))
                  .withColumn("PROV_PH_DTL", regexp_replace(trim(col("PROV_PH_DTL")), "^[ \\t]+|[ \\t]+$", ""))l
                  .withColumn("PROV_ADDR_DTL", split(col("PROV_ADDR_DTL"), "\\$").cast(ArrayType(StringType)))
                  .withColumn("PROV_ADDR_DTL", explode_outer(col("PROV_ADDR_DTL"))) 
                  .withColumn("PROV_ADDR_DTL", regexp_replace(trim(col("PROV_ADDR_DTL")), "^[ \\t]+|[ \\t]+$", ""))
                  .show(5)
                                
      
}