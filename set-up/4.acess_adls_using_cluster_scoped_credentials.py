# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data Lake using Cluster Scope Credentials
# MAGIC 1. Set the spark config fs.azure.account.key in cluster 
# MAGIC 2. list files from demo container
# MAGIC 3. Read the data from circuits file

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladlstorage1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladlstorage1.dfs.core.windows.net/circuits.csv"))