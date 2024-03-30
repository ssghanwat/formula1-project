-- Databricks notebook source
-- MAGIC %md
-- MAGIC ####lessons objective
-- MAGIC * Spark SQl documentation
-- MAGIC * create database demo
-- MAGIC * Data tab in UI
-- MAGIC * SHOW command
-- MAGIC * DESCRIBE command
-- MAGIC * Find the current Database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo ;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo ;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESCRIBE DATABASE demo

-- COMMAND ----------

DESCRIBE DATABASE EXTENDED demo

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SHOW TABLES IN  demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

SELECT current_database()

-- COMMAND ----------

SHOW TABLES


-- COMMAND ----------

use database default

-- COMMAND ----------

show tables

-- COMMAND ----------

show tables in default

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * create managed table using python
-- MAGIC * create managed table using SQL
-- MAGIC * Effect of dropping a managed table
-- MAGIC * Describe taable

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")
-- MAGIC

-- COMMAND ----------

drop table demo.race_results_python

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").format("delta").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

desc extended demo.race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year = 2020

-- COMMAND ----------

use demo

-- COMMAND ----------

create table if not exists race_results_sql
as 
select * from demo.race_results_python
where race_year = 2020

-- COMMAND ----------

select * from demo.race_results_sql

-- COMMAND ----------

desc extended demo.race_results_sql

-- COMMAND ----------

drop table demo.race_results_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC * Create External table using Python
-- MAGIC * Create External table using SQL
-- MAGIC * Effect of dropping an external table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.mode("overwrite").option("path", f"{presentation_folder_path}/race_results_ext_py").format("parquet").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

desc extended demo.race_results_ext_py

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Create External Table using SQL

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS DEMO.RACE_RESULTS_EXT_SQL
(
  RACE_YEAR INT,
    RACE_NAME STRING,
      RACE_DATE TIMESTAMP,
        CIRCUIT_LOCATION STRING,
          DRIVER_NAME STRING,
            DRIVER_NUMBER INT,
              DRIVER_NATIONALITY STRING,
                TEAM STRING,
                  GRID INT,
                    FASTEST_LAP INT,
                      RACE_TIME STRING,
                        POINTS FLOAT,
                          POSITION INT,
                            CREATED_DATE TIMESTAMP
)USING PARQUET
LOCATION "/mnt/f1adlsgen2storage/presentation/RACE_RESULTS_EXT_SQL"


-- COMMAND ----------

show tables in demo

-- COMMAND ----------

 insert into demo.RACE_RESULTS_EXT_SQL
 select * from RACE_RESULTS_EXT_py where race_year = 2020

-- COMMAND ----------

select count(1) from demo.RACE_RESULTS_EXT_SQL

-- COMMAND ----------

select  * from demo.RACE_RESULTS_EXT_SQL

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

drop table demo.RACE_RESULTS_EXT_SQL

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###views on tables 
-- MAGIC * Create temp view
-- MAGIC * Create global temp view
-- MAGIC * create permanent view

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW V_RACE_RESULTS
AS
SELECT * FROM DEMO.race_results_python
WHERE RACE_YEAR = 2018;

-- COMMAND ----------

SELECT * FROM V_RACE_RESULTS

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW GV_RACE_RESULTS
AS
SELECT * FROM DEMO.race_results_python
WHERE RACE_YEAR = 2012;

-- COMMAND ----------

SELECT * FROM GLOBAL_TEMP.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW  DEMO.PV_RACE_RESULTS
AS
SELECT * FROM DEMO.race_results_python
WHERE RACE_YEAR = 2000;

-- COMMAND ----------

SELECT * FROM DEMO.PV_RACE_RESULTS

-- COMMAND ----------

SHOW TABLES IN DEMO

-- COMMAND ----------

show databases

-- COMMAND ----------

show tables in f1_raw

-- COMMAND ----------


