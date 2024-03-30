# Databricks notebook source
# MAGIC %md
# MAGIC ####1.access_adls_using_access_keys
# MAGIC 1.set spark confg fs.azure.account.key
# MAGIC 2.list files from demo container
# MAGIC 3.Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

f1adlsgen2storage_account_key = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'f1adlsgen2storage-access-key' )

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.f1adlsgen2storage.dfs.core.windows.net",f1adlsgen2storage_account_key)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/"))

# COMMAND ----------



# COMMAND ----------

 display(spark.read.csv("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------

df = spark.read.csv("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/circuits.csv", header=True)

# COMMAND ----------

df.write.parquet("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/results")

# COMMAND ----------


