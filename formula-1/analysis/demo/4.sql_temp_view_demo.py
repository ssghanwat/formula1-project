# Databricks notebook source
# MAGIC %md
# MAGIC ####Access Dataframes using SQL
# MAGIC ######1.create temporary views on dataframe
# MAGIC ######2.Access the view from SQl cell
# MAGIC ######3.Access the view from python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(1)
# MAGIC FROM V_RACE_RESULTS
# MAGIC WHERE RACE_YEAR = 2020;

# COMMAND ----------

p_race_year = 2020


# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GLOBAL_TEMP.gv_race_results

# COMMAND ----------


