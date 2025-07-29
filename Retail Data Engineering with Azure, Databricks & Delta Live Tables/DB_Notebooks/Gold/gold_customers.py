# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text('init_load_flag','1')

# COMMAND ----------

init_load_flag = int(dbutils.widgets.get('init_load_flag'))

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Data Reading**

# COMMAND ----------

df = spark.sql("select * from databricks_cata.silver.customers")
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Removing Duplicates**

# COMMAND ----------

df = df.dropDuplicates(subset=["customer_id"])
df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Dividing New and Old**

# COMMAND ----------

if init_load_flag == 0:
    df_old = spark.sql('''select DimCustomerkey, customer_id, create_date, update_date
                       from databricks_cata.gold.dimcustomers''')

else:
    df_old = spark.sql('''select 0 DimCustomerkey,0 customer_id,0 create_date, 0 update_date
                       from databricks_cata.silver.customers where 1=0
                       ''')


# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Renaming the column names**

# COMMAND ----------

df_old = df_old.withColumnRenamed('DimCustomerkey','old_DimCustomerKey')\
                .withColumnRenamed('customer_id','old_customer_id')\
                .withColumnRenamed('create_date','old_create_date')\
                .withColumnRenamed('update_date','old_update_date')

# COMMAND ----------

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Applying join with old **

# COMMAND ----------

df_join = df.join(df_old,df['customer_id']==df_old['old_customer_id'],'left')
df_join.display() 

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Seperating old and new**

# COMMAND ----------

df_new = df_join.filter(col('old_DimCustomerKey').isNull())
df_new.display()

# COMMAND ----------

df_old = df_join.filter(col('old_DimCustomerKey').isNotNull())
df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing DF_old**

# COMMAND ----------

#droping the columns witch are not required
df_old = df_old.drop('old_customer_id','old_update_date')

#renaming old_dimcustomerkey to dimcustomerkey
df_old = df_old.withColumnRenamed('old_DimCustomerKey','DimCustomerkey')

#renaming old_create_date to create_date
df_old = df_old.withColumnRenamed('old_create_date','create_date')
df_old = df_old.withColumn('create_date',to_timestamp('create_date'))

#adding the update_date 
df_old = df_old.withColumn('update_date',current_timestamp())

df_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Preparing DF_New**

# COMMAND ----------

#removing unwanted columns
df_new = df_new.drop('old_DimCustomerKey','old_customer_id','old_create_date','old_update_date')

#Recreating update date and create date
df_new = df_new.withColumn('create_date',current_timestamp())
df_new = df_new.withColumn('update_date',current_timestamp())

df_new.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Surrogate Key**

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",monotonically_increasing_id()+lit(1))
df_new.display()

# COMMAND ----------

if init_load_flag == 1:
    max_surogate_key = 0

else:
    max_key = spark.sql('''select max(DimCustomerKey) as max_kay from databricks_cata.gold.dimcustomers''')
    max_surogate_key = max_key.collect()[0][0]

# COMMAND ----------

df_new = df_new.withColumn("DimCustomerKey",lit(max_surogate_key)+col("DimCustomerKey"))
df_new.display()

# COMMAND ----------

df_final = df_new.unionByName(df_old)
df_final.display()

# COMMAND ----------

from delta.tables import *

# COMMAND ----------

if spark.catalog.tableExists("databricks_cata.gold.dimcustomers"):
    delt_tab = DeltaTable.forPath(spark,"abfss://gold@storageproje2.dfs.core.windows.net/dimcustomers")
    
    delt_tab.alias('tar').merge(df_final.alias('src'), "tar.DimCustomerkey=src.DimCustomerKey")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
            .execute()


else:
    df_final.write.format('delta')\
        .mode('overwrite')\
            .option("path","abfss://gold@storageproje2.dfs.core.windows.net/dimcustomers")\
                .saveAsTable('databricks_cata.gold.dimcustomers')
            

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_cata.gold.dimcustomers