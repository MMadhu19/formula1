-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_drivers
AS
select driver_name,
  count(1) AS total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_point,
  rank() over(ORDER BY AVG(calculated_points) DESC) driver_rank
 from f1_presentation.calculated_race_results
 GROUP BY driver_name
 HAVING COUNT(1) >= 50
 ORDER BY avg_point DESC

-- COMMAND ----------

select race_year,
    driver_name,
    count(1) AS total_races,
    sum(calculated_points) as total_points,
    AVG(calculated_points) as avg_point
  from f1_presentation.calculated_race_results
  WHERE driver_name IN(SELECT driver_name from v_dominant_drivers where driver_rank <=10)
 GROUP BY race_year, driver_name
 ORDER BY race_year, avg_point DESC