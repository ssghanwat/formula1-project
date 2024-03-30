# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying json files

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                      StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("constructorId", IntegerType(), True),
                                      StructField("number", IntegerType(), True),
                                      StructField("position", IntegerType(), True),
                                      StructField("q1", StringType(), True),
                                      StructField("q2", StringType(), True),
                                      StructField("q3", StringType(), True),
                                     ])

# COMMAND ----------

qualifying_df = spark.read \
.schema(qualifying_schema) \
.option("multiLine", True) \
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Rename columns and add new columns
# MAGIC 1. Rename qualifyingId, driverId, constructorId and raceId
# MAGIC 1. Add ingestion_date with current timestamp

# COMMAND ----------

qualifying_with_ingestion_date_df = add_ingestion_date(qualifying_df)

# COMMAND ----------

final_df = qualifying_with_ingestion_date_df.withColumnRenamed("qualifyId", "qualify_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("data_source", lit(v_data_source))

# COMMAND ----------

# final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
# .withColumnRenamed("driverId", "driver_id") \
# .withColumnRenamed("raceId", "race_id") \
# .withColumnRenamed("constructorId", "constructor_id") \
# .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Write to output to processed container in parquet format

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_proccessed', 'qualifying', proccessed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# overwrite_partition(final_df, 'f1_proccessed', 'qualifying', 'race_id')

# COMMAND ----------

# final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_proccessed.qualifying")

# COMMAND ----------

# final_df.write.mode("overwrite").parquet(f"{proccessed_folder_path}/qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_proccessed.qualifying
# MAGIC group by race_id
# MAGIC order by race_id desc;

# COMMAND ----------


