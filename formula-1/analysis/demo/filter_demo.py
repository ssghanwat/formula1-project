# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------



# COMMAND ----------

races_df = spark.read.option("header", "true").parquet(f"{proccessed_folder_path}/races")

# COMMAND ----------

display(races_df)

# COMMAND ----------

races_filtered_df = races_df.filter((races_df["race_year"] == 2019) & (races_df["round"] <=5))
display(races_filtered_df)

# COMMAND ----------

 
