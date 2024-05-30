# Databricks notebook source
# MAGIC %md
# MAGIC ### Create folder in Databricks filestore -- apple-sale-analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Upload csv files in FileStore/apple-sale-analysis using UI from catalog section

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Create Delta table for Customers data to read it as a table
# MAGIC ##### Below is the code for reading csv and creating the delta table and querying the table.

# COMMAND ----------

# File location and type
file_location = "/FileStore/apple-sales-analysis/Customer_Updated.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# With this registered as a temp view, it will only be available to this particular notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
# Once saved, this table will persist across cluster restarts as well as allow various users across different notebooks to query this data.
# To do so, choose your table name and uncomment the bottom line.

permanent_table_name = "customer_delta_table"

df.write.format("parquet").saveAsTable(permanent_table_name)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC /* Query the created temp table in a SQL cell */
# MAGIC
# MAGIC select * from `Customer_delta_table`

# COMMAND ----------

#dbutils.fs.rm("dbfs:/user/hive/warehouse/customer_delta_table/", True)

# COMMAND ----------

