# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit_stops.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import lit, current_timestamp, col

# COMMAND ----------

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])

# COMMAND ----------

pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename driverId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

pit_stops_with_ingestion_date_df = add_ingestion_date(pit_stops_df)

# COMMAND ----------

final_df = pit_stops_with_ingestion_date_df.withColumnRenamed("driverId", "driver_id") \
                                          .withColumnRenamed("raceId", "race_id") \
                                          .withColumn("ingestion_date", current_timestamp()) \
                                          .withColumn("data_source", lit(v_data_source)) 

# COMMAND ----------

# final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
#                         .withColumnRenamed("raceId", "race_id") \
#                         .withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_proccessed', 'pit_stops', proccessed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# overwrite_partition(final_df, 'f1_proccessed', 'pit_stops', 'race_id')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_proccessed.pit_stops")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{proccessed_folder_path}/pit_stops")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_proccessed.results
# MAGIC group by race_id
# MAGIC order by race_id desc;
