# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake(managed table)
# MAGIC 2. Write data to delta lake(external table)
# MAGIC 3. Read data from delta lake(Table)
# MAGIC 4. Read data from delta lake(File)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "abfss://demo@formuladlstorage1.dfs.core.windows.net/"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json(f"{raw_folder_path}/2021-03-28/results.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Managed delta table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### External delta table

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("abfss://demo@formuladlstorage1.dfs.core.windows.net/results_external")

# COMMAND ----------

 # To read the data in delta format by creating a dataframe
 results_external_df = spark.read.format("delta").load("abfss://demo@formuladlstorage1.dfs.core.windows.net/results_external")

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC --To read the data in the delta format by creating a SQL table
# MAGIC CREATE TABLE f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION "abfss://demo@formuladlstorage1.dfs.core.windows.net/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

#paritioning in delta
results_df.write.format("delta").mode("overwrite").partitionBy('constructorId').saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_demo.results_partitioned

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update delta Table
# MAGIC 2. Delete delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11-position
# MAGIC   WHERE position <=10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, 'abfss://demo@formuladlstorage1.dfs.core.windows.net/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.update(
  condition = "position <= 10 ",
  set = { "points": "21-position" })


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Delete from a table

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.results_managed
# MAGIC   where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, 'abfss://demo@formuladlstorage1.dfs.core.windows.net/results_managed')

# Declare the predicate by using a SQL-formatted string.
deltaTable.delete("points = 0")



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##### Upsert using Merge

# COMMAND ----------



# COMMAND ----------

driver_day1_df = spark.read \
    .option("inferSchema",True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId <=10") \
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

driver_day1_df.createOrReplaceTempView("driver_day1")

# COMMAND ----------

display(driver_day1_df)

# COMMAND ----------

driver_day2_df = spark.read \
    .option("inferSchema",True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId  BETWEEN 6 AND 15") \
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(driver_day2_df)

# COMMAND ----------

driver_day2_df.createOrReplaceTempView("driver_day2")

# COMMAND ----------

driver_day3_df = spark.read \
    .option("inferSchema",True) \
    .json(f"{raw_folder_path}/2021-03-28/drivers.json") \
    .filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
    .select("driverId","dob","name.forename","name.surname")

# COMMAND ----------

display(driver_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING driver_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC     tgt.forename = upd.forename,
# MAGIC     tgt.surname = upd.surname,
# MAGIC     tgt.updateDate = current_timestamp()
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     createDate
# MAGIC   )
# MAGIC   VALUES (
# MAGIC     driverId,
# MAGIC     dob,
# MAGIC     forename,
# MAGIC     surname,
# MAGIC     current_timestamp()
# MAGIC   )

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from f1_demo.drivers_merge

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "abfss://demo@formuladlstorage1.dfs.core.windows.net/drivers_merge")

deltaTable.alias("tgt").merge(
    driver_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set =
    {
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "updateDate": "current_timestamp()"
    }
  ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename": "upd.forename",
      "surname": "upd.surname",
      "createDate": "current_timestamp()" 
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql Select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 3;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-08-31T16:58:25.000+0000'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf",'2023-08-31T16:58:25.000+0000').load('abfss://demo@formuladlstorage1.dfs.core.windows.net/drivers_merge')

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2023-08-31T16:46:05.000+0000'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId =1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge version as of 3 src
# MAGIC ON  (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Transaction Log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %md
# MAGIC Convert from Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename string,
# MAGIC   surname string,
# MAGIC   createDate DATE,
# MAGIC   updateDate DATE
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta;

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

# To convert a Parquet dataframe to delta 
df.write.format("parquet").save("abfss://demo@formuladlstorage1.dfs.core.windows.net/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC --To convert a Parquet dataframe to delta(Use backtick`` to enclose as we got folder names)
# MAGIC CONVERT TO DELTA parquet. `abfss://demo@formuladlstorage1.dfs.core.windows.net/demo/drivers_convert_to_delta_new`

# COMMAND ----------

