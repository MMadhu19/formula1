# Databricks notebook source
# MAGIC %md
# MAGIC ##### step 1: Reading the data from processed 

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# To read the data in delta format by creating a dataframe
races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
     .withColumnRenamed('name','race_name') \
     .withColumnRenamed('race_timestamp','race_date')

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits") \
    .withColumnRenamed('location','circuit_location')

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
    .withColumnRenamed('name','driver_name') \
    .withColumnRenamed('number','driver_number') \
    .withColumnRenamed('nationality','driver_nationality')

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
    .withColumnRenamed('name','team')

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
    .filter(f"file_date ='{v_file_date}'") \
    .withColumnRenamed('time','race_time')\
    .withColumnRenamed('race_id','result_race_id') \
    .withColumnRenamed("file_date","result_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join circuits to races

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_id == circuits_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name,races_df.race_date,circuits_df.circuit_location) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### join of all tables

# COMMAND ----------

 race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id) \
    .join(drivers_df, results_df.driver_id == drivers_df.driver_id) \
    .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id )

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

final_df = race_results_df.select("race_id","race_year","race_name","race_date","circuit_location","driver_name","driver_number","driver_nationality","team","grid","fastest_lap","race_time", "points","position","result_file_date") \
        .withColumn("create_date" , current_timestamp()) \
        .withColumnRenamed("result_file_date","file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Written data in parquet to presentation 

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
 
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')