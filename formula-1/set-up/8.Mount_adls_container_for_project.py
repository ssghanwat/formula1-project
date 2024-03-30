# Databricks notebook source
# MAGIC %md
# MAGIC ###1.Mount Azure Data Lake containers for project
# MAGIC
# MAGIC

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    #get secrets from key vault
    client_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-id')
    tenant_id = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-tenant-id')
    client_secret = dbutils.secrets.get(scope = 'formula1-secretScope', key = 'formula1-app-client-secret')

    #set saprk configurations
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    #unmount the mount if already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    
    #mount storage account container
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs)
    
    #list mounts
    display(dbutils.fs.mounts())


# COMMAND ----------

# MAGIC %md
# MAGIC ###mount Raw Container

# COMMAND ----------

mount_adls("f1adlsgen2storage","raw")

# COMMAND ----------

# MAGIC %md
# MAGIC ###mount presentation Container

# COMMAND ----------

mount_adls("f1adlsgen2storage","presentation")

# COMMAND ----------

# MAGIC %md
# MAGIC ###mount proccessed Container

# COMMAND ----------

mount_adls("f1adlsgen2storage","proccessed")

# COMMAND ----------


