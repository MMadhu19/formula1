# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest constructors.json File

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 1: read the constructors.json file using spark dataframe reader

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

constructors_schema = " constructorId INT, constructorRef STRING, name STRING, nationality  STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
.schema(constructors_schema) \
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### step 2: Select, Rename, Drop and ingestion of columns 

# COMMAND ----------

constructors_dropped_df = constructors_df.drop('url')

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed('constructorId','constructor_id') \
                                                .withColumnRenamed('constructorRef', 'constructor_ref') \
                                                .withColumn('data_source',lit(v_data_source)) \
                                                .withColumn('file_date',lit(v_file_date))

# COMMAND ----------

final_df = add_ingestion_date(constructors_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write the columns to the datalake in parquet format

# COMMAND ----------

final_df.write.mode('overwrite').format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

dbutils.notebook.exit('success')