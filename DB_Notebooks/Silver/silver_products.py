# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.read.format('parquet')\
    .load("abfss://bronze@storageproje2.dfs.core.windows.net/products")

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df.createOrReplaceTempView("products")

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Functions**

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.discount_func(p_price double)
# MAGIC returns double
# MAGIC language sql
# MAGIC return p_price * 0.90;

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,price,databricks_cata.bronze.discount_func(price) as discounted_price from products

# COMMAND ----------

df = df.withColumn("discounted_price",expr("databricks_cata.bronze.discount_func(price)"))
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace function databricks_cata.bronze.uppercase_func(p_string string)
# MAGIC returns string
# MAGIC language python
# MAGIC as
# MAGIC $$
# MAGIC   return p_string.upper() 
# MAGIC $$

# COMMAND ----------

# MAGIC %sql
# MAGIC select product_id,brand,databricks_cata.bronze.uppercase_func(brand) as brand_upper from products

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
        .save("abfss://silver@storageproje2.dfs.core.windows.net/products")

# COMMAND ----------

df = spark.read.format('delta')\
    .load("abfss://silver@storageproje2.dfs.core.windows.net/products")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.products
# MAGIC using delta
# MAGIC location "abfss://silver@storageproje2.dfs.core.windows.net/products"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.products