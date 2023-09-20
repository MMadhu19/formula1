# Databricks notebook source
# MAGIC %md
# MAGIC #### Access the dataframe using SQL
# MAGIC ##### step 1: creating the temp view
# MAGIC ##### step 2: accessing the view using sql
# MAGIC ##### step 3: accesing the view using python

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Creating the temp view

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select count(1) 
# MAGIC from v_race_results
# MAGIC where race_year = 2020;

# COMMAND ----------

py_df = spark.sql('select * from v_race_results where race_year = 2019')

# COMMAND ----------

display(py_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from global_temp.gv_race_results;

# COMMAND ----------

