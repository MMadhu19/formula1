# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. Read csv file using spark dataframe

# COMMAND ----------

dbutils.widgets.text('p_data_source', "")
v_data_source = dbutils.widgets.get('p_data_source')

# COMMAND ----------

dbutils.widgets.text('p_file_date', "")
v_file_date = dbutils.widgets.get('p_file_date')

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StringType, StructType, IntegerType,StructField,DateType

# COMMAND ----------

races_schema = StructType(fields = [StructField('raceId',IntegerType(),False),
                                    StructField('year',IntegerType(),True),
                                    StructField('round',IntegerType(),True),
                                    StructField('circuitId',IntegerType(),True),StructField('name',StringType(),True),
                                    StructField('date',DateType(), True),
                                    StructField('time',StringType(),True),
                                    StructField('url',StringType(),True)])


# COMMAND ----------

races_df = spark.read \
.option('header', True) \
.schema(races_schema) \
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step2: inserting the columns we needed

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat,current_timestamp,to_timestamp

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn('ingestion_date', current_timestamp()) \
                            .withColumn('race_timestamp',to_timestamp(concat(col('date'),lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss')) \
                                .withColumn('data_source', lit(v_data_source)) \
                                .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

races_ingestion_df = add_ingestion_date(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step3: selecting and renaming the columns we needed

# COMMAND ----------

races_final_df = races_ingestion_df.select(col('raceId').alias('race_id'),col('year').alias('race_year'), col('round'),col('circuitId').alias('circuit_id'),col('name'),col('race_timestamp'),col('ingestion_date'),col('data_source'), col('file_date'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step4: Write the data to datalake in Parquet

# COMMAND ----------

races_final_df.write.mode('overwrite').partitionBy('race_year').format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC  select * from f1_processed.races

# COMMAND ----------

dbutils.notebook.exit('success')