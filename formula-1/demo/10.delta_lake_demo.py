# Databricks notebook source
# MAGIC %md
# MAGIC 1. Write data to delta lake (managed table)
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (Table)
# MAGIC 4. Read data from delta lake (File)

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
        source = f"abfss:/demo/@f1adlsgen2storage.dfs.core.windows.net/",
        mount_point = f"/mnt/f1adlsgen2storage/demo",
        extra_configs = configs)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE  IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/f1adlsgen2storage/demo'

# COMMAND ----------

results_df = spark.read.option("inferSchema", "True").json("/mnt/f1adlsgen2storage/raw/2021-03-28/results.json")

# COMMAND ----------

# docs.delta.io    --------------> delta lake documentation
# docs.databricks.com ----------------> Databricks Documentation 

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# syntax to write data to file location 
results_df.write.format("delta").mode("overwrite").save("/mnt/f1adlsgen2storage/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS F1_DEMO.RESULTS_EXTERNAL
# MAGIC USING DELTA 
# MAGIC LOCATION "/mnt/f1adlsgen2storage/demo/results_external"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_EXTERNAL

# COMMAND ----------

result_external_df = spark.read.format("delta").load("/mnt/f1adlsgen2storage/demo/results_external")
# display(result_external_df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_partitioned")

# COMMAND ----------

# MAGIC %sql
# MAGIC show partitions f1_demo.results_partitioned
# MAGIC -- to show partions in  table

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update Delta Table
# MAGIC 2. Delete From Delta Table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC  update f1_demo.results_managed
# MAGIC  set points = 11 - position
# MAGIC  where position <= 10

# COMMAND ----------

from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/f1adlsgen2storage/demo/results_managed")

deltaTable.update("position <= 10", {"points" :"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %sql
# MAGIC  delete from f1_demo.results_managed
# MAGIC  where position > 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/f1adlsgen2storage/demo/results_managed")
deltaTable.delete("points = 0")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed

# COMMAND ----------

# MAGIC %md
# MAGIC Upsert using merge

# COMMAND ----------

drivers_day1_df = spark.read\
  .option("infeSchema", "True")\
    .json("/mnt/f1adlsgen2storage/raw/2021-03-28/drivers.json")\
      .filter("driverId <= 10")\
        .select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

# %sql
# drop view drivers_day1

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from drivers_day1

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day2_df = spark.read\
  .option("infeSchema", "True")\
    .json("/mnt/f1adlsgen2storage/raw/2021-03-28/drivers.json")\
      .filter("driverId between 6 and 15")\
        .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# %sql 
# drop view drivers_day2

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

drivers_day3_df = spark.read\
  .option("infeSchema", "True")\
    .json("/mnt/f1adlsgen2storage/raw/2021-03-28/drivers.json")\
      .filter("driverId between 1 and 5 or driverId between 16 and 20")\
        .select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- DROP table f1_demo.drivers_merge;
# MAGIC  CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge(
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createDate  DATE,
# MAGIC   updateDate DATE
# MAGIC  )
# MAGIC  USING DELTA

# COMMAND ----------

# MAGIC %md Day1
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge TGT
# MAGIC USING drivers_day1 UPD
# MAGIC ON TGT.driverId = UPD.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET TGT.dob = UPD.dob,
# MAGIC               TGT.forename = UPD.forename,
# MAGIC               TGT.surname = UPD.surname,
# MAGIC               TGT.updateDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.DRIVERS_MERGE

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC Day 2

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from drivers_day2

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge TGT
# MAGIC USING drivers_day2 UPD
# MAGIC ON TGT.driverId = UPD.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET TGT.dob = UPD.dob,
# MAGIC               TGT.forename = UPD.forename,
# MAGIC               TGT.surname = UPD.surname,
# MAGIC               TGT.updateDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.DRIVERS_MERGE
# MAGIC order BY driverId

# COMMAND ----------

# MAGIC %md
# MAGIC Day 3

# COMMAND ----------

drivers_day3_df.createOrReplaceTempView("drivers_day3")

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp

deltaTable = DeltaTable.forPath(spark, "/mnt/f1adlsgen2storage/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId")\
      .whenMatchedUpdate(set = {"dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updateDate" : current_timestamp()})\
      .whenNotMatchedInsert(values = {
        "driverId" : "upd.driverId",
        "dob" : "upd.dob",
        "forename" : "upd.forename", 
        "surname" : "upd.forename",
        "createDate" : "current_timestamp()"
        }
                            ).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge TGT
# MAGIC USING drivers_day3 UPD
# MAGIC ON TGT.driverId = UPD.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET TGT.dob = UPD.dob,
# MAGIC               TGT.forename = UPD.forename,
# MAGIC               TGT.surname = UPD.surname,
# MAGIC               TGT.updateDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC   THEN INSERT (driverId, dob, forename, surname, createDate) VALUES (driverId, dob, forename, surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge
# MAGIC order by driverId

# COMMAND ----------

# MAGIC %md
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 3

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-03-29T04:44:21Z'

# COMMAND ----------

df = spark.read.format("delta").option("timestampAsOf", '2024-03-29T04:44:21Z').load("/mnt/f1adlsgen2storage/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC vacuum f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-03-29T04:44:21Z'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- if we want to delete data immediately without having versioning to see data back
# MAGIC set spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC vacuum f1_demo.drivers_merge retain 0 hours

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge timestamp as of '2024-03-29T04:44:21Z'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge 

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId = 1;

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge version as of 1
# MAGIC ;

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction Logs

# COMMAND ----------

# MAGIC %sql
# MAGIC   CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn(
# MAGIC     driverId INT,
# MAGIC     dob  DATE,
# MAGIC     forename STRING,
# MAGIC     surname STRING,
# MAGIC     createDate DATE,
# MAGIC     updatedDate DATE
# MAGIC   )
# MAGIC   USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC desc history  f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into f1_demo.drivers_txn
# MAGIC select * from f1_demo.drivers_merge
# MAGIC where driverId = 1

# COMMAND ----------

# MAGIC %md
# MAGIC Convert Parquet to Delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta(
# MAGIC    driverId INT,
# MAGIC     dob  DATE,
# MAGIC     forename STRING,
# MAGIC     surname STRING,
# MAGIC     createDate DATE,
# MAGIC     updatedDate DATE
# MAGIC )
# MAGIC using PARQUET

# COMMAND ----------

 
