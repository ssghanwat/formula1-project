-- Databricks notebook source
use f1_presentation

-- COMMAND ----------

desc driver_standings

-- COMMAND ----------

 CREATE OR REPLACE TEMP VIEW V_DRIVER_STANDINGS_2018
 AS 
 SELECT RACE_YEAR, DRIVER_NAME, TEAM, TOTAL_POINTS, WINS, RANK
 FROM driver_standings
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM V_DRIVER_STANDINGS_2018

-- COMMAND ----------

   CREATE OR REPLACE TEMP VIEW V_DRIVER_STANDINGS_2020
 AS 
 SELECT RACE_YEAR, DRIVER_NAME, TEAM, TOTAL_POINTS, WINS, RANK
 FROM driver_standings
 WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM V_DRIVER_STANDINGS_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####inner join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  join v_driver_standings_2020 d_2020
  on(d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####lefft join
-- MAGIC

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  left join v_driver_standings_2020 d_2020
  on(d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ####right join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  right join v_driver_standings_2020 d_2020
  on(d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md semi join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  semi join v_driver_standings_2020 d_2020
  on(d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md anti

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  anti join v_driver_standings_2020 d_2020
  on(d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###cross join

-- COMMAND ----------

select *
  from v_driver_standings_2018 d_2018
  cross join v_driver_standings_2020 d_2020

-- COMMAND ----------


