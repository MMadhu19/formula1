# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest Results.json file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### step 1 - Read the json file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_data_store", "")
v_data_store = dbutils.widgets.get("p_data_store")

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId", IntegerType(), False),
                                   StructField("raceId", IntegerType(), True), 
                                   StructField("driverId", IntegerType(), True),
                                   StructField("constructorId", IntegerType(), True),
                                   StructField("number", IntegerType(), True),
                                   StructField("grid", IntegerType(), True),
                                   StructField("position", IntegerType(), True),
                                   StructField("positionText", StringType(), True),
                                   StructField("positionOrder", IntegerType(), True),
                                   StructField("points", FloatType(), True),
                                   StructField("laps", IntegerType(), True),
                                   StructField("time", StringType(), True),
                                   StructField("milliseconds", IntegerType(), True),
                                   StructField("fastestLap", IntegerType(), True),
                                   StructField("rank", IntegerType(), True),
                                   StructField("fastestLapTime", StringType(), True),
                                   StructField("fastestLapSpeed", FloatType(), True),
                                   StructField("statusId", StringType(), True)])

# COMMAND ----------

results_df = spark.read \
.schema(results_schema) \
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 - Renaming and adding ingestion date column  and concat name as forename surname

# COMMAND ----------

from pyspark.sql.functions import concat, lit, current_timestamp

# COMMAND ----------

results_selected_df = results_df.withColumnRenamed('resultId' , 'result_id') \
                                .withColumnRenamed('raceId','race_id') \
                                .withColumnRenamed('driverId','driver_id') \
                                .withColumnRenamed('constructorId','constructor_id') \
                                .withColumnRenamed('positionText','postion_text') \
                                .withColumnRenamed('positionOrder', 'position_order') \
                                .withColumnRenamed('fastestLap','fastest_lap') \
                                .withColumnRenamed('fastestLapTime','fastest_lap_time') \
                                .withColumnRenamed('fastestLapSpeed','fastest_lap_speed') \
                                .withColumn("data_source", lit(v_data_store)) \
                                .withColumn("file_date", lit(v_file_date))


# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3 - Dropping url column

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_dropped_df = results_selected_df.drop(col('statusId')) 

# COMMAND ----------

results_final_df = add_ingestion_date(results_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC De-Dupe the dataframe(droppping the duplicates)

# COMMAND ----------

results_deduped_df = results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
 
merge_delta_data(results_deduped_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from f1_processed.results where race_id = 540 and driver_id = 229 

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit("success")