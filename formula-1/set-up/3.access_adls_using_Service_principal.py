# Databricks notebook source
# MAGIC %md
# MAGIC ####1.access_adls_using_Service_principal
# MAGIC #####steps :
# MAGIC ######1.Register Azure AD Application/ Service Principal
# MAGIC ######2.Generate secret/password fro the application 
# MAGIC ######3.Set spark config wuth App/Client Id, Directory/Tenant Id & secret
# MAGIC ######4.Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-tenant-id')
client_secret = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-secret')

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.f1adlsgen2storage.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.f1adlsgen2storage.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.f1adlsgen2storage.dfs.core.windows.net",client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.f1adlsgen2storage.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.f1adlsgen2storage.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@f1adlsgen2storage.dfs.core.windows.net/circuits.csv", header=True))

# COMMAND ----------


