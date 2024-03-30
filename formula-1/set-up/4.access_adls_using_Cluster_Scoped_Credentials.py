# Databricks notebook source
# MAGIC %md
# MAGIC ####1.access_adls_using_Cluster_Scoped_Credentials
# MAGIC ######1.set spark confg fs.azure.account.key in the cluster
# MAGIC ######2.list files from demo container
# MAGIC ######3.Read data from circuits.csv file
# MAGIC

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/"))

# COMMAND ----------

 display(spark.read.csv("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------


