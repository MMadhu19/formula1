-- Databricks notebook source
select driver_name,
  count(1) AS total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_point
 from f1_presentation.calculated_race_results
 GROUP BY driver_name
 HAVING COUNT(1) >= 50
 ORDER BY avg_point DESC