# Databricks notebook source
# DBTITLE 1,FALCON_STG_CUSTOMER
# MAGIC  %run /Users/ghoraipabitrakumar@gmail.com/Project_Falcon_Sales/FALCON_STG_CUSTOMER

# COMMAND ----------

# DBTITLE 1,FALCON_STG_DATE
# MAGIC  %run /Users/ghoraipabitrakumar@gmail.com/Project_Falcon_Sales/FALCON_STG_DATE

# COMMAND ----------

# DBTITLE 1,FALCON_STG_MARKET
# MAGIC  %run /Users/ghoraipabitrakumar@gmail.com/Project_Falcon_Sales/FALCON_STG_MARKET

# COMMAND ----------

# DBTITLE 1,FALCON_STG_PRODUCTS
# MAGIC  %run /Users/ghoraipabitrakumar@gmail.com/Project_Falcon_Sales/FALCON_STG_PRODUCTS

# COMMAND ----------

# DBTITLE 1,FALCON_STG_TRANSACTION
# MAGIC  %run /Users/ghoraipabitrakumar@gmail.com/Project_Falcon_Sales/FALCON_STG_TRANSACTION

# COMMAND ----------

# Define a list of notebooks to run
notebooks = ['FALCON_STG_CUSTOMER', 'FALCON_STG_DATE', 'FALCON_STG_MARKET']

# Define a dictionary to store the results
results = {}

# Loop through the notebooks and run them
for notebook in notebooks:
    try:
        # Run the notebook and get the return value
        retValue = dbutils.notebook.run (notebook)
        # Store the return value in the results dictionary
        results [notebook] = retValue
    except Exception as e:
        # Store the error message in the results dictionary
        results [notebook] = f"Error: {e}"

# Print the results dictionary
print (results)
