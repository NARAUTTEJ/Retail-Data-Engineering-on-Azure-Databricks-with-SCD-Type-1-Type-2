# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import * 


# COMMAND ----------

df = spark.read.format('parquet')\
    .load("abfss://bronze@storageproje2.dfs.core.windows.net/customers")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df = df.withColumn("domain",split('email','@').getItem(1))
df.display()

# COMMAND ----------

df.groupBy('domain').agg(count('customer_id').alias('total_customers')).sort('total_customers',ascending=False).display()

# COMMAND ----------

df_gmail = df.filter(col('domain')=='gmail.com')
df_gmail.display() 

df_hotmail = df.filter(col('domain')=="hotmail.com")
df_hotmail.display()

df_yahoo = df.filter(col('domain')=="yahoo.com")
df_yahoo.display()

# COMMAND ----------

df = df.withColumn('fullname',concat('first_name',lit(' '),'last_name'))
df = df.drop('first_name','last_name')
df.display()

# COMMAND ----------

df.write.format('delta').mode('overwrite').save('abfss://silver@storageproje2.dfs.core.windows.net/customers')

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.customers
# MAGIC using delta
# MAGIC location "abfss://silver@storageproje2.dfs.core.windows.net/customers"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.customers