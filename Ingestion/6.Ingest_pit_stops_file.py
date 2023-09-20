# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest pit_stops.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step1: Read json using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField('raceId', IntegerType(), False),
                                        StructField('driverId', IntegerType(), True), 
                                        StructField('stop', StringType(), True),
                                        StructField('lap', IntegerType(), True),
                                        StructField('time',  StringType(), True),
                                        StructField('duration', StringType(), True),
                                        StructField('milliseconds', IntegerType(), True)])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine",True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename and ingestion of new columns to data

# COMMAND ----------

pit_stops_ingested_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_final_df = pit_stops_ingested_df.withColumnRenamed('raceId','race_id') \
                                        .withColumnRenamed('driverId','driver_id') \
                                        .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3: write data to the datalake in parquet format

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
 
merge_delta_data(pit_stops_final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.pit_stops
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit('success')