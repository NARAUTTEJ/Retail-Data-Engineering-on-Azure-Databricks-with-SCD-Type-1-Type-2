# Databricks notebook source
# MAGIC %md
# MAGIC # **Fact Orders**

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Reading Data**

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.orders")
df.display()

# COMMAND ----------

df_dimcus = spark.sql('''select customer_id as dim_customer_id, DimCustomerkey from databricks_cata.gold.dimcustomers''')
df_dimcus.display()

# COMMAND ----------

df_dimpro = spark.sql("select product_id as dim_product_id from databricks_cata.gold.dim_products")
df_dimpro.display()

# COMMAND ----------

df_fact = df.join(df_dimcus, df['customer_id']==df_dimcus['dim_customer_id'],how='left')\
            .join(df_dimpro, df['product_id']==df_dimpro['dim_product_id'],how='left')
df_fact.display()

# COMMAND ----------

df_fact_new = df_fact.drop('customer_id','product_id','dim_customer_id')
df_fact_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Upsert**

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.factorders"):
    del_tabl = DeltaTable.forName(spark,"databricks_cata.gold.factorders")
    del_tabl.alias("tar").merge(df_fact_new.alias('src').dropDuplicates(['order_id', 'DimCustomerkey', 'dim_product_id']), "tar.order_id = src.order_id and tar.DimCustomerkey = src.DimCustomerkey and tar.dim_product_id = src.dim_product_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
            .execute()

else:
    df_fact_new.write.format('delta')\
        .mode('overwrite')\
            .option("path","abfss://gold@storageproje2.dfs.core.windows.net/factorders")\
                .saveAsTable("databricks_cata.gold.factorders")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.factorders

# COMMAND ----------

