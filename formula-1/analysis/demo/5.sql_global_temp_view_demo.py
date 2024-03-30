# Databricks notebook source
# MAGIC %md
# MAGIC ####global Temporary View
# MAGIC ######1.create global temporary views on dataframe
# MAGIC ######2.Access the view from SQl cell
# MAGIC ######3.Access the view from python cell
# MAGIC ######4.Access the view from another notebook
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN global_temp

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM GLOBAL_TEMP.GV_RACE_RESULTS
# MAGIC WHERE RACE_YEAR = 2020;

# COMMAND ----------

p_race_year = 2020


# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from GLOBAL_TEMP.v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

df.write.mode("ovrewrite").format(parquet).save("..///.")
