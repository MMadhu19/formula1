# Databricks notebook source
# Databricks notebook source
dbutils.notebook.run("1.Ingest_circuits_file", 0, {"p_data_source": "Ergast API", "p_file_date":"2021-03-21"})

# COMMAND ----------

dbutils.notebook.run("2.Ingest_races_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("3.Ingest_constructors_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("4.Ingest_drivers_file", 0, {"p_data_source": "Ergast API","p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("5.Ingest_results_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("6.Ingest_pit_stops_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("7.Ingest_lap_times_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})

# COMMAND ----------

 dbutils.notebook.run("8.Ingest_qualifying_file", 0, {"p_data_source": "Ergast API", "p_file_date": "2021-03-21"})