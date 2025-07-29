# Databricks notebook source
dbutils.widgets.text("file_name","")

# COMMAND ----------

p_file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

df = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation",f"abfss://bronze@storageproje2.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .load(f"abfss://source@storageproje2.dfs.core.windows.net/{p_file_name}")

# COMMAND ----------

df.writeStream.format("parquet")\
    .outputMode("append")\
    .option("checkpointLocation", f"abfss://bronze@storageproje2.dfs.core.windows.net/checkpoint_{p_file_name}")\
    .option("path", f"abfss://bronze@storageproje2.dfs.core.windows.net/{p_file_name}")\
    .trigger(once=True)\
    .start()

# COMMAND ----------

df = spark.read.format('parquet')\
    .load(f"abfss://bronze@storageproje2.dfs.core.windows.net/{p_file_name}")

df.display()

# COMMAND ----------

