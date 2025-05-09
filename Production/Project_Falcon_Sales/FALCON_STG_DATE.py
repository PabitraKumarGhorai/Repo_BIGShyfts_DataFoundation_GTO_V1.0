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
df = spark.read.format('csv').option("header","true").option('inferSchema','true').load(f"{mnt_raw}/date.csv")
display(df)

# COMMAND ----------

df1 = df.filter(df.cy_date.isNotNull())
display(df1)
df1.createOrReplaceTempView("TEMP_DATE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_DATE

# COMMAND ----------

sql_str = '''
select
date,
cy_date,
year,
month_name,
date_yy_mmm
from
TEMP_DATE
'''
df2 = spark.sql(sql_str)
df2.createOrReplaceTempView("TEMP_STG_DATE")
df2.write.format("delta").mode("overwrite").save(f"{mnt_enriched}/Falcon_Sales/FALCON_STG_DATE")
df2.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_DATE")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_DATE

# COMMAND ----------

