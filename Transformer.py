# Databricks notebook source
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, col, broadcast,collect_set,size,array_contains,rank,avg,datediff, row_number

class Transformer:
    def __init__(self):
        pass
    def transform(self, inputDFs):
        pass


# COMMAND ----------

class AirpodsAfterIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought Airpods after buying iPhone
        """

        #get transactionInputDF from inputDF dictionary passed to function
        transactionInputDF=inputDFs.get("transactionInputDF") 

        print("TransactioninputDF in transform")
        transactionInputDF.show()

        #LEAD (product_name)-> Partition by customer_id and order by transaction date asc
        windowSpec=Window.partitionBy("customer_id").orderBy("transaction_date")
        transformedDF= transactionInputDF.withColumn(
            "next_product_name", lead("product_name").over(windowSpec)
        )

        print("Airpods after buying iPhone")
        transformedDF.orderBy("customer_id","transaction_date").show()

        #Filter on product_name and next_product_name
        filteredDF=transformedDF.filter(
            (col("product_name")=="iPhone") & (col("next_product_name")=="AirPods") )
        filteredDF.show()

        #get customerInputDF from inputDF dictionary passed to function
        customerInputDF=inputDFs.get("customerInputDF")
        print("customerInputDF in transform")
        customerInputDF.show()

        #Join Customer and Transaction to select required columns
        joinDF=customerInputDF.join(
            filteredDF,"customer_id"
        )

        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id","customer_name","location")

# COMMAND ----------

class OnlyAirpodsAndIphoneTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Customers who have bought only Airpods and iPhone
        """

        transactionInputDF=inputDFs.get("transactionInputDF") 
        print("TransactioninputDF in transform")
        transactionInputDF.show()

        groupedDF=transactionInputDF.groupBy("customer_id").agg(
            collect_set("product_name").alias("products")
        )

        print("groupedDF in transform")
        groupedDF.show()

        filteredDF=groupedDF.filter(
            (array_contains(col("products"),"iPhone")) & 
            (array_contains(col("products"),"AirPods")) &
            (size(col("products"))==2))
             
        print("groupedDF in transform")
        filteredDF.show()

        #get customerInputDF from inputDF dictionary passed to function
        customerInputDF=inputDFs.get("customerInputDF")
        print("customerInputDF in transform")
        customerInputDF.show()

        #Join Customer and Transaction to select required columns
        joinDF=customerInputDF.join(
            broadcast(filteredDF),"customer_id"
        )

        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id","customer_name","location")


# COMMAND ----------

class ProductsAfterInitialPurchaseTransformer(Transformer):

    def transform(self, inputDFs):
        """
        All products bought after initial purchase by customer
        """

        transactionInputDF=inputDFs.get("transactionInputDF") 
        print("TransactioninputDF in transform")
        transactionInputDF.show()

        #Rank-> Partition by customer_id and order by transaction date asc
        windowSpec=Window.partitionBy("customer_id").orderBy("transaction_date")
        transformedDF= transactionInputDF.withColumn(
            "tnxs_order", rank().over(windowSpec)
        )

        print("Order of products bought")
        transformedDF.orderBy("customer_id","transaction_date").show()

        #Filter on product_name and next_product_name
        filteredDF=transformedDF.filter((col("tnxs_order")!=1))
        filteredDF.show()

        #get customerInputDF from inputDF dictionary passed to function
        customerInputDF=inputDFs.get("customerInputDF")
        print("customerInputDF in transform")
        customerInputDF.show()

        #Join Customer and Transaction to select required columns
        joinDF=customerInputDF.join(
            filteredDF,"customer_id"
        )

        print("Joined DF")
        joinDF.show()

        return joinDF.select(
            "customer_id","customer_name","product_name")




# COMMAND ----------

class AvgLagTimeTransformer(Transformer):

    def transform(self, inputDFs):
        """
        Avg Lag time for Customers who have bought Airpods after buying iPhone
        """

        #get transactionInputDF from inputDF dictionary passed to function
        transactionInputDF=inputDFs.get("transactionInputDF") 

        print("TransactioninputDF in transform")
        transactionInputDF.show()

        #LEAD (product_name)-> Partition by customer_id and order by transaction date asc
        windowspec=Window.partitionBy("customer_id","product_name").orderBy("transaction_date")
        transformDf=transactionInputDF.withColumn("row_number",row_number().over(windowspec)).filter((col("product_name")=="iPhone") | (col("product_name")=="AirPods") & (col("row_number")==1)) 
        
        print("Transformed df in AverageTimeDelayTransformer class with row_num ")
        transformDf.show(truncate=False)
        print("newtransform df in AverageTimeDelayTransformer class")
        window=Window.partitionBy("customer_id").orderBy("transaction_date")
        transformDf2=transformDf.withColumn("next_transaction_date",lead("transaction_date").over(window)).filter(col("next_transaction_date").isNotNull())\
            .withColumn("lag_time",datediff("next_transaction_date","transaction_date"))

        print("result of newtransform df in AverageTimeDelayTransformer class")
        transformDf2.show(truncate=False)

        return transformDf2.select(avg("lag_time").alias("avg_delay_iphone_airpods_in_days"))

# COMMAND ----------

