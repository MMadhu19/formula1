# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times.csv files using spark dataframe reader API

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date','')
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema= StructType(fields = [StructField('raceId' , IntegerType(), False),
                                       StructField('driverId', IntegerType(), True),
                                       StructField('lap', IntegerType(), True),
                                       StructField('position', IntegerType(), True),StructField('time', StringType(), True), StructField('milliseconds', IntegerType(),True)])

# COMMAND ----------

lap_times_df = spark.read \
.schema(lap_times_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_df.withColumnRenamed('raceId','race_id') \
                       .withColumnRenamed('driverId','driver_id') \
                        .withColumn('data_source', lit(v_data_source))

# COMMAND ----------

lap_times_final_df = add_ingestion_date(final_df)

# COMMAND ----------

merge_condition = "tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
 
merge_delta_data(lap_times_final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id,count(1)
# MAGIC from f1_processed.lap_times
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------

dbutils.notebook.exit('success')