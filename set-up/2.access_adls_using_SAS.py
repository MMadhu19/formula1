# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure datalake storage using SAS tokens
# MAGIC 1. Set spark configuration config.fs.azure.account.key
# MAGIC 2. List all the file from the container
# MAGIC 3. Read data from circuits file

# COMMAND ----------

formula1_demo_sas_token = dbutils.secrets.get(scope = 'formula1-scope', key = 'formula1-demo-sas-token')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formuladlstorage1.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formuladlstorage1.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formuladlstorage1.dfs.core.windows.net",formula1_demo_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formuladlstorage1.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formuladlstorage1.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------

