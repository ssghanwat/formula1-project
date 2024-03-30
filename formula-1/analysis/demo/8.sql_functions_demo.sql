-- Databricks notebook source
use f1_proccessed;

-- COMMAND ----------

select  *,concat(driver_ref,"-",code ) as new_driver_ref from f1_proccessed.drivers

-- COMMAND ----------

select  *,split(name, ' ')[0] as forename from f1_proccessed.drivers

-- COMMAND ----------

select *, current_timestamp()
from f1_proccessed.drivers

-- COMMAND ----------

select *, date_format(dob, 'dd-MM-yyyy') as dob
from f1_proccessed.drivers

-- COMMAND ----------

select  max(dob) from f1_proccessed.drivers

-- COMMAND ----------

select  * from f1_proccessed.drivers
where dob = '2000-05-11'

-- COMMAND ----------


select * from f1_proccessed.drivers
where dob in(select max(dob) from f1_proccessed.drivers)

-- COMMAND ----------

select count(*) from f1_proccessed.drivers 
where nationality = 'British'

-- COMMAND ----------

SELECT NATIONALITY, COUNT(*)
FROM f1_proccessed.drivers
GROUP BY nationality
HAVING count(*) > 100
ORDER BY nationality;

-- COMMAND ----------

SELECT nationality,NAME, DOB, RANK() OVER (PARTITION BY NATIONALITY ORDER BY DOB DESC) AS AGE_RANK
   FROM f1_proccessed.drivers
   ORDER BY nationality, AGE_RANK;

-- COMMAND ----------

 
