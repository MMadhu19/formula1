-- Databricks notebook source
select team_name,
  count(1) AS total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_point
 from f1_presentation.calculated_race_results
 where race_year BETWEEN 2001 and 2011
 GROUP BY team_name
 HAVING COUNT(1) >= 100
 ORDER BY avg_point DESC

-- COMMAND ----------

