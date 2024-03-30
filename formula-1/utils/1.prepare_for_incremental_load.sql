-- Databricks notebook source
-- MAGIC  %md
-- MAGIC ##### Drop all the tables

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_proccessed CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_proccessed
LOCATION "/mnt/f1adlsgen2storage/proccessed";

-- COMMAND ----------

DROP DATABASE IF EXISTS f1_presentation CASCADE;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/f1adlsgen2storage/presentation";

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------


