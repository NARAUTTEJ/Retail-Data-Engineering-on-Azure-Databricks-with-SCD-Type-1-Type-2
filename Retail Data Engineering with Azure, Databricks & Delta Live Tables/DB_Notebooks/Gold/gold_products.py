# Databricks notebook source
# MAGIC %md
# MAGIC #DLT Pipeline

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
import dlt

# COMMAND ----------

# Expectations
my_rules = {
    "rule1" : "product_id IS NOT NULL",
    "rule2" : "product_name IS NOT NULL"
}

# COMMAND ----------

@dlt.table()

@dlt.expect_all_or_drop(my_rules)
def dim_products_stage(): 

  df = spark.readStream.option("skipChangeCommits","true")\
                       .table("databricks_cata.silver.products")

  return df

# COMMAND ----------

@dlt.view 

def dim_products_view():

    df = spark.readStream.table("dim_products_stage")
    return df 

# COMMAND ----------

dlt.create_streaming_table("dim_products")

# COMMAND ----------

dlt.apply_changes(
  target = "dim_products",
  source = "dim_products_view",
  keys = ["product_id"],
  sequence_by = "product_id",
  stored_as_scd_type = 2
)