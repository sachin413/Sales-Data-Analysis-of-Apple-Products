# Databricks notebook source
# MAGIC %run "./loader_factory"

# COMMAND ----------

class AbstractLoader:
    def __init__(self, transformDF):
        self.transformDF= transformDF

    def sink(self):
        pass

class AirpodsAfterIphoneLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type="dbfs",
            df=self.transformDF,
            path="dbfs:/FileStore/tables/apple-sales-analysis/output/airpodsafteriphone", 
            method="overwrite"
        ).load_data_frame()

class OnlyAirpodsAndIphoneLoader(AbstractLoader):

    def sink(self):
        params={
            "partitionByColumns":["location"]
        }
        get_sink_source(
            sink_type="dbfs_with_partition",
            df=self.transformDF,
            path="dbfs:/FileStore/tables/apple-sales-analysis/output/onlyairpodsandiphone", 
            method="overwrite",
            params=params
        ).load_data_frame()

        get_sink_source(
            sink_type="delta",
            df=self.transformDF,
            path="default.onlyairpodsandiphone", 
            method="overwrite"
        ).load_data_frame()


class ProductsAfterInitialPurchaseLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type="delta",
            df=self.transformDF,
            path="default.productsafterinitialpurchase", 
            method="overwrite"
        ).load_data_frame()


class AvgLagTimeLoader(AbstractLoader):

    def sink(self):
        get_sink_source(
            sink_type="delta",
            df=self.transformDF,
            path="default.avglagtime", 
            method="overwrite"
        ).load_data_frame()
