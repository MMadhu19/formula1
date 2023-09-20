-- Databricks notebook source
-- MAGIC %md
-- MAGIC #### Lesson Objective
-- MAGIC 1. Spark SQL documentation
-- MAGIC 2. Create database demo
-- MAGIC 3. SHOW Command
-- MAGIC 4. DESCRIBE Command
-- MAGIC 5. find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE database EXTENDED demo;

-- COMMAND ----------

show current database;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objective
-- MAGIC 1. Create Managed table using python
-- MAGIC 2. Create Managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. DESCRIBE table

-- COMMAND ----------

-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

USE demo;
SHOW tables;

-- COMMAND ----------

DESC EXTENDED race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------


CREATE TABLE race_results_sql
AS
SELECT * 
  FROM race_results_python
  WHERE race_year = 2020 

-- COMMAND ----------


select current_database()

-- COMMAND ----------

desc extended demo.race_results_sql;

-- COMMAND ----------

show tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Lesson Objective
-- MAGIC 1. Create External table using python
-- MAGIC 2. Create External table using SQL
-- MAGIC 3. Effect of dropping a external table
-- MAGIC 4. DESCRIBE table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

drop table  race_results_ext_py;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("parquet").option("path", f"{presentation_folder_path}/race_results").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

select current_database()

-- COMMAND ----------

use demo;

-- COMMAND ----------

show tables

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql(
race_year int,
race_name string,
race_date timestamp,
circuit_location string,
driver_name string,
driver_number int,
driver_nationality string,
team string,
grid int,
fastest_lap int,
race_time string,
points float,
position int,
ingestion_date timestamp
)
USING PARQUET
LOCATION "abfss://presentation@formuladlstorage1.dfs.core.windows.net/race_results_ext_sql"

-- COMMAND ----------

Insert into demo.race_results_ext_sql 
select * from demo.race_results_ext_py where race_year = 2020;

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### View on Tables
-- MAGIC 1. create Temp View
-- MAGIC 2. Create Global Temp view
-- MAGIC 3. Create permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

select * from  v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2021

-- COMMAND ----------

show tables in global_temp;

-- COMMAND ----------

select * from global_temp.gv_race_results;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2019

-- COMMAND ----------

show tables in demo;

-- COMMAND ----------

select * from demo.pv_race_results;

-- COMMAND ----------

