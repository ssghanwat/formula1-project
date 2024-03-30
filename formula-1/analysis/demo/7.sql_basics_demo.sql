-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

USE f1_proccessed

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM f1_proccessed.drivers LIMIT 10;

-- COMMAND ----------

DESC EXTENDED f1_proccessed.drivers

-- COMMAND ----------

SELECT * FROM f1_proccessed.drivers
WHERE nationality = 'British' and dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob FROM f1_proccessed.drivers
WHERE nationality = 'British' and dob >= '1990-01-01'

-- COMMAND ----------

SELECT name, dob FROM f1_proccessed.drivers
WHERE nationality = 'British' and dob >= '1990-01-01'
ORDER By dob DESC;

-- COMMAND ----------

select * from f1_proccessed.drivers
order by nationality asc, dob desc

-- COMMAND ----------

SELECT name, dob,nationality FROM f1_proccessed.drivers
WHERE (nationality = 'British' and dob >= '1990-01-01') or (nationality='India')
ORDER BY dob DESC

-- COMMAND ----------


