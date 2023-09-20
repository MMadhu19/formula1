# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark join Transformation

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
    .filter('circuit_id < 70') \
    .withColumnRenamed('name','circuit_name')

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
    .withColumnRenamed('name','race_name')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Inner join

# COMMAND ----------

#inner Join
race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"inner")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Left outer join

# COMMAND ----------

#Left outer Join
race_circuits_left_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"left")

# COMMAND ----------

display(race_circuits_left_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Right outer Join

# COMMAND ----------

#Right outer Join
race_circuits_right_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"right")

# COMMAND ----------

display(race_circuits_right_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Full Outer join

# COMMAND ----------

#Full outer Join
race_circuits_full_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"full")

# COMMAND ----------

display(race_circuits_full_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Semi join(only matching and gives left table columns)

# COMMAND ----------

#semi Join
race_circuits_semi_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"semi")

# COMMAND ----------

display(race_circuits_semi_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Cross Join
# MAGIC

# COMMAND ----------

#cross Join
race_circuits_cross_df = circuits_df.crossJoin(races_df)

# COMMAND ----------

display(race_circuits_cross_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Anti Join(Unmatched and retrives left table columns)
# MAGIC

# COMMAND ----------

#Anti Join
race_circuits_anti_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id,"anti")

# COMMAND ----------

display(race_circuits_anti_df)