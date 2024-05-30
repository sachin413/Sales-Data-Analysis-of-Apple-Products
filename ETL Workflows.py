# Databricks notebook source
# MAGIC %run "./Transformer"

# COMMAND ----------

# MAGIC %run "./Extractor"

# COMMAND ----------

# MAGIC %run "./Loader"

# COMMAND ----------

class FirstWorkFlow:
    """
    ETL Pipeline to generate data for all customers who have bought airpods after buying iphone
    """

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs= AirpodsAfterIphoneExtractor().extract()

        #Step 2: Transformation logic -Customers who have bought Airpods after buying iPhone
        firstTransformDF=AirpodsAfterIphoneTransformer().transform(inputDFs)

        #Step 3: Load all required data to different sink
        AirpodsAfterIphoneLoader(firstTransformDF).sink()

#firstWorkFlow=FirstWorkFlow().runner()


# COMMAND ----------

class SecondWorkFlow:
    """
    ETL Pipeline to generate data for all customers who have bought only Airpods and iPhone
    """

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs= AirpodsAfterIphoneExtractor().extract()

        #Step 2: Transformation logic -Customers who have bought Airpods after buying iPhone
        secondTransformDF=OnlyAirpodsAndIphoneTransformer().transform(inputDFs)

        #Step 3: Load all required data to different sink
        OnlyAirpodsAndIphoneLoader(secondTransformDF).sink()

#secondWorkFlow=SecondWorkFlow().runner()


# COMMAND ----------

class ThirdWorkFlow:
    """
    ETL Pipeline to generate data for all Products bought after initlal purchase by all customers 
    """

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs= AirpodsAfterIphoneExtractor().extract()

        #Step 2: Transformation logic -Customers who have bought Airpods after buying iPhone
        thirdTransformDF= ProductsAfterInitialPurchaseTransformer().transform(inputDFs)

        #Step 3: Load all required data to different sink
        ProductsAfterInitialPurchaseLoader(thirdTransformDF).sink()


#thirdWorkFlow=ThirdWorkFlow().runner()


# COMMAND ----------

class FourthWorkFlow:
    """
    ETL Pipeline to generate data for all Products bought after initlal purchase by all customers 
    """

    def __init__(self):
        pass

    def runner(self):

        #Step 1: Extract all required data from different sources
        inputDFs= AirpodsAfterIphoneExtractor().extract()

        #Step 2: Transformation logic -Customers who have bought Airpods after buying iPhone
        fourthTransformDF= AvgLagTimeTransformer().transform(inputDFs)

        #Step 3: Load all required data to different sink
        AvgLagTimeLoader(fourthTransformDF).sink()


#fourthdWorkFlow=FourthWorkFlow().runner()

# COMMAND ----------

