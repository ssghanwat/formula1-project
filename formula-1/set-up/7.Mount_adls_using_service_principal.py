# Databricks notebook source
# MAGIC %md
# MAGIC ####1.Mount Azure Data Lake using_Service_principal
# MAGIC #####steps to follow :
# MAGIC ######1.Get client-id, tenant-id, client-secret from key-vault
# MAGIC ######2.Set spark config wuth App/Client Id, Directory/Tenant Id & secret
# MAGIC ######3.Call the file System utility mount to mount the storage
# MAGIC ######4.Explore other file System Utilities related to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@f1adlsgen2storage.dfs.core.windows.net/",
  mount_point = "/mnt/f1adlsgen2storage/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/f1adlsgen2storage/demo"))

# COMMAND ----------

 display(spark.read.csv("dbfs:/mnt/f1adlsgen2storage/demo/circuits.csv", header=True))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# dbutils.fs.unmount("/mnt/dlgen2storage2419f1/demo")

# COMMAND ----------


