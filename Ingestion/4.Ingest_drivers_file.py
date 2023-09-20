# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1: read the driver.json file 

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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType( fields=[StructField('forename', StringType(), True),
                                    StructField('surname', StringType(), True)])

# COMMAND ----------

driver_schema = StructType( fields = [StructField('driverId', IntegerType(), False),
                                      StructField('driverRef', StringType(), True),
                                      StructField('number', IntegerType(), True),
                                      StructField('code', StringType(), True),
                                      StructField('name',name_schema),
                                      StructField('dob', DateType(), True ),
                                      StructField('nationality', StringType(), True),
                                      StructField('url', StringType(), True)])

# COMMAND ----------

drivers_df = spark.read \
.option('header', True) \
.schema(driver_schema) \
.json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2: select,rename and drop unwanted columns

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

driver_final_df = drivers_df.withColumnRenamed('driverId','driver_id') \
                            .withColumnRenamed('driverRef', 'driver_ref') \
                            .withColumn('name', concat(col('name.forename'), lit(' '),col('name.surname'))) \
                            .withColumn('data_source',lit(v_data_source)) \
                            .withColumn('file_date', lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(driver_final_df)

# COMMAND ----------

dr_final_df = final_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 3: write the data in datalake as parquet files

# COMMAND ----------

dr_final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_processed.drivers

# COMMAND ----------

dbutils.notebook.exit('success')