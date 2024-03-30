-- Databricks notebook source
DROP database f1_presentation;
CREATE DATABASE IF NOT EXISTS f1_presentation 
LOCATION "/mnt/f1adlsgen2storage/presentation"

-- COMMAND ----------

desc   database extended f1_presentation

-- COMMAND ----------

desc extended f1_presentation.race_results

