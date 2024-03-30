# Databricks notebook source
raw_folder_path = "/mnt/f1adlsgen2storage/raw"
proccessed_folder_path = "/mnt/f1adlsgen2storage/proccessed"
presentation_folder_path = "/mnt/f1adlsgen2storage/presentation"

# COMMAND ----------

# raw_folder_path = "abfss://raw@f1adlsgen2storage.dfs.core.windows.net"
# proccessed_folder_path = "abfss://proccessed@f1adlsgen2storage.dfs.core.windows.net"
# presentation_folder_path = "abfss://presentation@f1adlsgen2storage.dfs.core.windows.net"

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------


