# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore_DBFS_Root 
# MAGIC ######1.List All files in DBFS Root.
# MAGIC ######2.Interacty with DBFS File Browser.
# MAGIC ######3.upload File to DBFS Root.

# COMMAND ----------

display(dbutils.fs.ls('/'))

# COMMAND ----------

 dbutils.fs.ls("dbfs:/FileStore/")

# COMMAND ----------

df =spark.read.csv('/FileStore/tables/circuits1.csv', header= True)
display(df)

# COMMAND ----------


