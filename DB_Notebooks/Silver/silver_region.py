# Databricks notebook source
df = spark.read.table('databricks_cata.bronze.regions')

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.drop('_rescued_data')
df.display()

# COMMAND ----------

df.write.format('delta')\
    .mode('overwrite')\
        .save("abfss://silver@storageproje2.dfs.core.windows.net/regions")

# COMMAND ----------

df = spark.read.format('delta')\
    .load("abfss://silver@storageproje2.dfs.core.windows.net/orders")
df.display()

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists databricks_cata.silver.regions
# MAGIC using delta
# MAGIC location "abfss://silver@storageproje2.dfs.core.windows.net/regions"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.silver.regions 

# COMMAND ----------

