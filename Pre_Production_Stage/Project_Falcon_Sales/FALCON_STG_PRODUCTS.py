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

df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load(f"{mnt_raw}/falcon_sales_files/products.csv")
display(df1)


# COMMAND ----------

#This Line of code is use to remove all null values which is coming in each new row

df2 = df1.filter(df1.product_type.isNotNull())
display(df2)
df2.createOrReplaceTempView("TEMP_PRODUCTS")

# COMMAND ----------

sql_str = '''
select
"INDIA" as COUNTRY_NAME,
"ASIA" as REGION,
product_code,
product_type
from TEMP_PRODUCTS
'''
df = spark.sql(sql_str)
df.createOrReplaceTempView("TEMP_STG_PRODUCTS")
df.write.format("delta").mode("overwrite").save(f"{mnt_enriched}/Falcon_Sales/FALCON_STG_PRODUCTS")
df.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_PRODUCTS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_PRODUCTS

# COMMAND ----------

