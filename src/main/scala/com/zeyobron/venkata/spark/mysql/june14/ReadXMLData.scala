package com.zeyobron.venkata.spark.mysql.june14

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode
import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.functions.collect_set
import org.apache.spark.sql.functions.array
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ReadXMLData extends App{
   Logger.getLogger("org").setLevel(Level.ERROR)
   val spark = SparkSession.builder().appName("ReadXMLData").master("local[*]").getOrCreate()
   val file_path = "file:///K://ZEYOBRON//DATA//"
   val df = spark.read.format("xml").option("rootTag", "POSLog").option("rowTag", "Transaction").load(file_path+"transactions.xml")
   df.printSchema()
   
   val df1 = df.withColumn("ControlTransaction_array",explode(array(col("ControlTransaction.OperatorSignOff.*"))))
               .withColumn("ReasonCode", explode(array(col("ControlTransaction.ReasonCode"))))
               .withColumn("Version", explode(array(col("ControlTransaction._Version"))))
               .withColumn("OperatorName", explode(array(col("OperatorID._OperatorName"))))
               .withColumn("OperatorValue", explode(array(col("OperatorID._VALUE"))))
               .withColumn("LineItems",explode(col("RetailTransaction.LineItem")))
               .withColumn("Sale_Description",explode(array(col("LineItems.Sale.Description"))))
               .withColumn("Sale_DiscountAmount",explode(array(col("LineItems.Sale.DiscountAmount"))))
               .withColumn("Sale_ExtendedAmount",explode(array(col("LineItems.Sale.ExtendedAmount"))))
               .withColumn("Sale_ExtendedDiscountAmount",explode(array(col("LineItems.Sale.ExtendedDiscountAmount"))))
               .withColumn("Sale_ItemID",explode(array(col("LineItems.Sale.ItemID"))))
               .withColumn("Total",explode(col("RetailTransaction.Total")))
               .withColumn("ItemCount",col("RetailTransaction.ItemCount"))
               .withColumn("Sale_Itemizers",explode(array(col("LineItems.Sale.Itemizers"))))
               .withColumn("Sale_Itemizers_FoodStampable",explode(array(col("Sale_Itemizers._FoodStampable"))))
               .withColumn("Sale_Itemizers_Itemizer6",explode(array(col("Sale_Itemizers._Itemizer6"))))
               .withColumn("Sale_Itemizers_Itemizer8",explode(array(col("Sale_Itemizers._Itemizer8"))))
               .withColumn("Sale_Itemizers_Tax1",explode(array(col("Sale_Itemizers._Tax1"))))
               .withColumn("Sale_Itemizers_Value",explode(array(col("Sale_Itemizers._VALUE"))))
               .withColumn("Sale_MerchandiseHierarchy",explode(array(col("LineItems.Sale.MerchandiseHierarchy"))))
               .withColumn("Sale_MerchandiseHierarchy_DepartmentDescription",explode(array(col("Sale_MerchandiseHierarchy._DepartmentDescription"))))
               .withColumn("Sale_MerchandiseHierarchy_Level",explode(array(col("Sale_MerchandiseHierarchy._Level"))))
               .withColumn("Sale_MerchandiseHierarchy_Value",explode(array(col("Sale_MerchandiseHierarchy._VALUE"))))
               .withColumn("Sale_OperatorSequence",explode(array(col("LineItems.Sale.OperatorSequence"))))
               .withColumn("Sale_OperatorSequence",explode(array(col("LineItems.Sale.OperatorSequence"))))
               .withColumn("Sale_POSIdentity",explode(array(col("LineItems.Sale.POSIdentity"))))
               .withColumn("Sale_POSIdentity_POSItemID",explode(array(col("Sale_POSIdentity.POSItemID"))))
               .withColumn("Sale_POSIdentity_Qualifier",explode(array(col("Sale_POSIdentity.Qualifier"))))
               .withColumn("Sale_POSIdentity_POSIDType",explode(array(col("Sale_POSIdentity._POSIDType"))))
               .withColumn("Sale_RegularSalesUnitPrice",explode(array(col("LineItems.Sale.RegularSalesUnitPrice"))))
               .withColumn("Sale_ReportCode",explode(array(col("LineItems.Sale.ReportCode"))))
               .withColumn("Sale_ItemType",explode(array(col("LineItems.Sale._ItemType"))))
               .withColumn("LineItems_SequenceNumber",explode(array(col("LineItems.SequenceNumber"))))
               .withColumn("LineItems_Tax",explode(array(col("LineItems.Tax.*"))))
               .withColumn("LineItems_Tender",explode(array(col("LineItems.Tender"))))
               .withColumn("LineItems_Tender_Authorization",explode(array(col("LineItems_Tender.Authorization"))))
               .withColumn("LineItems_Tender_Authorization_AuthorizationCode",explode(array(col("LineItems_Tender_Authorization.AuthorizationCode"))))
               .withColumn("LineItems_Tender_Authorization_AuthorizationDateTime",explode(array(col("LineItems_Tender_Authorization.AuthorizationDateTime"))))
               .withColumn("LineItems_Tender_Authorization_ReferenceNumber",explode(array(col("LineItems_Tender_Authorization.ReferenceNumber"))))
               .withColumn("LineItems_Tender_Authorization_RequestedAmount",explode(array(col("LineItems_Tender_Authorization.RequestedAmount"))))
               .withColumn("LineItems_Tender_Authorization_ElectronicSignature",explode(array(col("LineItems_Tender_Authorization._ElectronicSignature"))))
               .withColumn("LineItems_Tender_Authorization_HostAuthorized",explode(array(col("LineItems_Tender_Authorization._HostAuthorized"))))
               .withColumn("LineItems_Tender_OperatorSequence",explode(array(col("LineItems_Tender.OperatorSequence"))))
               .withColumn("LineItems_Tender_TenderID",explode(array(col("LineItems_Tender.TenderID"))))
               .withColumn("LineItems_Tender_TenderDescription",explode(array(col("LineItems_Tender._TenderDescription"))))
               .withColumn("LineItems_Tender_TenderType",explode(array(col("LineItems_Tender._TenderType"))))
               .withColumn("LineItems_Tender_TypeCode",explode(array(col("LineItems_Tender._TypeCode"))))
               .withColumn("LineItems_EntryMethod",explode(array(col("LineItems._EntryMethod"))))
               .withColumn("LineItems_weightItem",explode(array(col("LineItems._weightItem"))))
               .withColumn("PerformanceMetrics_IdleTime",explode(array(col("RetailTransaction.PerformanceMetrics.IdleTime"))))
               .withColumn("PerformanceMetrics_RingTime",explode(array(col("RetailTransaction.PerformanceMetrics.RingTime"))))
               .withColumn("PerformanceMetrics_TenderTime",explode(array(col("RetailTransaction.PerformanceMetrics.TenderTime"))))
               .withColumn("RetailTransaction_ReceiptDateTime",explode(array(col("RetailTransaction.ReceiptDateTime"))))
               .withColumn("RetailTransaction_Total",explode(col("RetailTransaction.Total")))
               .withColumn("RetailTransaction_Total_TotalType", explode(array(col("RetailTransaction_Total._TotalType"))))
               .withColumn("RetailTransaction_Total_VALUE",explode(array(col("RetailTransaction_Total._VALUE"))))
               .withColumn("RetailTransaction_TransactionCount",explode(array(col("RetailTransaction.TransactionCount"))))
               .withColumn("RetailTransaction_Version",explode(array(col("RetailTransaction._Version"))))
          .select("*")
                
    print("*************************")
    df1.show(2,false)
    val df2 = df1
    
    df2.show(2,false)
    
    val df3 = df2.drop("OperatorID").drop("RetailTransaction").drop("Total").drop("RetailTransaction_Total")
    df3.show(2,false)
   val columns_count =df3.schema.fieldNames.toList.size
   println(s"Number of columns in the XML after extracting ===> ${columns_count}")
   println(s"Number of Rows/Data in the XML after extracting ===>  ${df3.count()}")
}