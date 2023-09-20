# Databricks notebook source
# MAGIC %md
# MAGIC #### Access the dataframe using SQL
# MAGIC ##### step 1: creating the global view
# MAGIC ##### step 2: accessing the view using sql
# MAGIC ##### step 3: accesing the view using python
# MAGIC ##### step 3: accesing the from another notebook

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

df.createOrReplaceGlobalTempView('gv_race_results')

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from global_temp.gv_race_results where race_year = 2019

# COMMAND ----------

spark.sql("select * from global_temp.gv_race_results").show()

# COMMAND ----------

