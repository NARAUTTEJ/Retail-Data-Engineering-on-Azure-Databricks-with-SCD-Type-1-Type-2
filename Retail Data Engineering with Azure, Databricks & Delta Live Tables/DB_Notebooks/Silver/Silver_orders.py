# Databricks notebook source
from pyspark.sql.functions import * 
from pyspark.sql.types import * 
from pyspark.sql.window import Window

# COMMAND ----------

df = spark.read.format('parquet')\
    .load("abfss://bronze@storageproje2.dfs.core.windows.net/orders")

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df  = df.withColumnRenamed('_rescued_data','rescued_data')

# COMMAND ----------

df = df.drop('rescued_data')
df.display()

# COMMAND ----------

df = df.withColumn("order_date",to_timestamp(col("order_date")))
df.display()

# COMMAND ----------

df = df.withColumn("Year",year(col('order_date')))
df.display()

# COMMAND ----------

df1 = df.withColumn('flag',dense_rank().over(Window.partitionBy("Year").orderBy(desc('total_amount'))))
df1.display()

# COMMAND ----------

df2 = df1.withColumn('rank_flag',rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
df2.display()

# COMMAND ----------

df3 = df2.withColumn('row_flag',row_number().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
df3.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Class- OOP**

# COMMAND ----------

class windows:

    def dense_rank(self,df):

        df = df.withColumn('flag',dense_rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
        return df
    
    def rank(self,df):

        df = df.withColumn('rank_flag',rank().over(Window.partitionBy('Year').orderBy(desc('total_amount'))))
        return df
    def row_number(self,df):

        df = df.withColumn('row_number',row_number().over(Window.partitionBy('Year').orderBy(desc('total_amount')))) 
        return df
    


# COMMAND ----------

df_new = df

# COMMAND ----------

obj = windows()

# COMMAND ----------

df_result = obj.rank(df_new)
df_result.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing into silver layer

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
    .save("abfss://silver@storageproje2.dfs.core.windows.net/orders")

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.orders
# MAGIC using delta
# MAGIC location "abfss://silver@storageproje2.dfs.core.windows.net/orders"

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from databricks_cata.silver.orders