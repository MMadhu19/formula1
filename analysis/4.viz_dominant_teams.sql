-- Databricks notebook source
-- MAGIC %python
-- MAGIC html = """<h1 style="color:Black;text-align:center;font-family:Ariel">Report on Dominant Formula 1 Driver</h1>"""
-- MAGIC displayHTML(html)

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_dominant_teams
AS
select team_name,
  count(1) AS total_races,
  sum(calculated_points) as total_points,
  AVG(calculated_points) as avg_point,
  rank() over(ORDER BY AVG(calculated_points) DESC) team_rank
 from f1_presentation.calculated_race_results
 GROUP BY team_name
 HAVING COUNT(1) >= 50
 ORDER BY avg_point DESC

-- COMMAND ----------

select race_year,
    team_name,
    count(1) AS total_races,
    sum(calculated_points) as total_points,
    AVG(calculated_points) as avg_point
  from f1_presentation.calculated_race_results
  WHERE team_name IN(SELECT team_name from v_dominant_teams where team_rank <=5)
 GROUP BY race_year, team_name
 ORDER BY race_year, avg_point DESC