# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Aggregation functions demo

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

from pyspark.sql.functions import col
demo_df = race_results_df.filter(col("race_year") == 2020)
display(demo_df)

# COMMAND ----------

from pyspark.sql.functions import col,count, countDistinct,sum, max, min, avg, mean

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.select(sum("points")).show()

# COMMAND ----------

demo_df.filter(col("driver_name" ) == 'Lewis Hamilton').select(sum("points"), countDistinct("race_name"))\
                                        .withColumnRenamed("sum(points)", "total_points")\
                                            .withColumnRenamed("count(DISTINCT race_name)", "number of races").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####group by
# MAGIC
# MAGIC

# COMMAND ----------

demo_df.groupBy("driver_name").agg(sum("points").alias("total_points"), countDistinct("race_name").alias("total_number_of_races")).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###window

# COMMAND ----------

from pyspark.sql.functions import col
demo_df = race_results_df.filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_grouped_df = demo_df\
    .groupBy("race_year","driver_name")\
        .agg(sum("points").alias("total_points"),\
             countDistinct("race_name").alias("total_number_of_races"))\
                 

# COMMAND ----------

display(demo_grouped_date)

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, col, rank

driverRankspec = Window.partitionBy(col("race_year")).orderBy(col("total_points").desc())

rank_df = demo_grouped_df.withColumn("rank", rank().over(driverRankspec))

# COMMAND ----------

display(rank_df)

# COMMAND ----------


