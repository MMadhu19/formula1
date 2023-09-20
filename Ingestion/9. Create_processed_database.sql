-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_processed
LOCATION "abfss://processed@formuladlstorage1.dfs.core.windows.net/"

-- COMMAND ----------

DESC DATABASE f1_raw

-- COMMAND ----------

