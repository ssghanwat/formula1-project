# Databricks notebook source
# MAGIC %md
# MAGIC ##Explore capabilities of dbutils.secrets utilities

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list(scope = 'formula1-secretScope')

# COMMAND ----------

dbutils.secrets.get(scope = 'formula1-secretScope', key = 'f1adlsgen2storage-access-key' )

# COMMAND ----------


