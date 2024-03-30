# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races")

# COMMAND ----------

races_filtered_df = races_df.filter("race_year = 2019 and round <= 5")

# COMMAND ----------

races_filtered_df = races_df.where((races_df["race_year"] == 2019) & (races_df["round"] <= 5))

# COMMAND ----------

display(races_filtered_df)

# COMMAND ----------



# COMMAND ----------

# Repair table to import partition metadata into the metastore
spark.sql("MSCK REPAIR TABLE f1_proccessed.results")

# Loop through race_id_list and drop partitions
for race_id_list in results_final_df.select("race_id").distinct().collect():
    spark.sql(f"ALTER TABLE f1_proccessed.results DROP IF EXISTS PARTITION (race_id = '{race_id_list.race_id}')")
