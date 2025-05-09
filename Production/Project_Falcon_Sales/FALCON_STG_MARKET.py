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

df1 = spark.read.format('csv').option("header","true").option('inferSchema','true').load(f"{mnt_raw}/markets.csv")
display(df1)
df1.createOrReplaceTempView("TEMP_MARKETS")


# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC "INDIA" as COUNTRY_NAME,
# MAGIC "ASIA" as REGION,
# MAGIC markets_code,
# MAGIC markets_name,
# MAGIC zone
# MAGIC from TEMP_MARKETS
# MAGIC where zone != 'NULL'

# COMMAND ----------

sql_str = '''
SELECT
"INDIA" as COUNTRY_NAME,
"ASIA" as REGION,
markets_code,
markets_name,
zone
from TEMP_MARKETS
where zone != "NULL"
'''
df1 = spark.sql(sql_str)
df1.createOrReplaceTempView("TEMP_STG_MARKETS")
df1.write.format("delta").mode("overwrite").save(f"{mnt_enriched}/Falcon_Sales/FALCON_STG_MARKETS")
df1.write.format('delta').mode('overwrite').saveAsTable("FALCON_STG_MARKETS")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from TEMP_STG_MARKETS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from FALCON_STG_MARKETS

# COMMAND ----------

