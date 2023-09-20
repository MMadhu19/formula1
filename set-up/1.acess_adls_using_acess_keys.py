# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Access keys
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. list files from demo container
# MAGIC 3. Read the data from circuits file

# COMMAND ----------

formuladl_account_key = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1dl-account-key')

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.formuladlstorage1.dfs.core.windows.net",formuladl_account_key)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladlstorage1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladlstorage1.dfs.core.windows.net/circuits.csv"))