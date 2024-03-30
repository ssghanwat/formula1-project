# Databricks notebook source
# MAGIC %md
# MAGIC ####1.access_adls_using_SAS_Token
# MAGIC 1.set spark confg SAS_Token
# MAGIC 2.list files from demo container
# MAGIC 3.Read data from circuits.csv file

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

sas_token = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-demo-SAS-Token' )

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1adlsgen2storage.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.f1adlsgen2storage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.f1adlsgen2storage.dfs.core.windows.net",sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/"))

# COMMAND ----------

 display(spark.read.csv("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------


