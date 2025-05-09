# Databricks notebook source
clear cache

# COMMAND ----------

notebook_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
print(notebook_path)
workspace_name = notebook_path.split('/')[3]
print(workspace_name)

# COMMAND ----------

# MAGIC %run /Commons/Utilities/environment_setup 

# COMMAND ----------

env_details=set_env()

mnt_raw = env_details.get('mnt_raw')
mnt_curated = env_details.get('mnt_curated')
mnt_enriched = env_details.get('mnt_enriched')
print(mnt_raw)


# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("DataFrame").getOrCreate()
df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load(f"{mnt_raw}/customers.csv")
display(df1)
df1.createOrReplaceTempView("TEMP_CUSTOMER")


# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 'INDIA' as COUNTRY_NAME,
# MAGIC 'ASIA' as REGION,
# MAGIC customer_code as CUSTOMER_CODE,
# MAGIC custmer_name as CUSTOMER_NAME,
# MAGIC customer_type as CUSTOMER_TYPE
# MAGIC from 
# MAGIC TEMP_CUSTOMER

# COMMAND ----------

str_sql = '''
select
'INDIA' as COUNTRY_NAME,
'ASIA' as REGION,
customer_code as CUSTOMER_CODE,
custmer_name as CUSTOMER_NAME,
customer_type as CUSTOMER_TYPE
from 
TEMP_CUSTOMER
'''
df1 = spark.sql(str_sql)
df1.createOrReplaceTempView("TEMP_STG_CUTOMER")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_STG_CUTOMER

# COMMAND ----------


df1.write.format("delta").mode("overwrite").save(f"{mnt_enriched}/Falcon_Sales/FALCON_STG_CUSTOMER")
df1.write.format("delta").mode("overwrite").saveAsTable('FALCON_STG_CUSTOMER')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_CUSTOMER

# COMMAND ----------

