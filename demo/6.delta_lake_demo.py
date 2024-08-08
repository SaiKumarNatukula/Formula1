# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import *
from delta import *
import pandas as pd
from pyspark.sql.functions import row_number
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_demo
# MAGIC LOCATION "/mnt/18formula1dl/demo"

# COMMAND ----------

results_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/18formula1dl/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").saveAsTable("f1_demo.results_managed_test")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/18formula1dl/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_externals
# MAGIC USING DELTA
# MAGIC LOCATION "/mnt/18formula1dl/demo/results_external/"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_externals;

# COMMAND ----------

df=spark.read.format("delta").load("/mnt/18formula1dl/demo/results_external/")

# COMMAND ----------

display(df)

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy("constructorId").saveAsTable("f1_demo.results_p")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW partitions f1_demo.results_p

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1_demo.results_managed
# MAGIC   SET points = 11 - position
# MAGIC WHERE position <= 10

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %md Merge

# COMMAND ----------

drivers_day1_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/18formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId <= 10") \
.select("driverId", "dob", "name.forename", "name.surname")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1_df")

# COMMAND ----------

display(drivers_day1_df)

# COMMAND ----------

from pyspark.sql.functions import upper, col

drivers_day2_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/18formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 6 AND 15") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

drivers_day2_df.createOrReplaceTempView("drivers_day2_df")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

from pyspark.sql.functions import upper

drivers_day3_df = spark.read \
.option("inferSchema", True) \
.json("/mnt/18formula1dl/raw/2021-03-28/drivers.json") \
.filter("driverId BETWEEN 1 AND 5 OR driverId BETWEEN 16 AND 20") \
.select("driverId", "dob", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"))

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge_test(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1_df upd
# MAGIC ON tgt.DriverId=upd.DriverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE into f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2_df upd
# MAGIC ON tgt.driverId=upd.driverId
# MAGIC WHEN MATCHED THEN
# MAGIC   UPDATE SET
# MAGIC     tgt.dob = upd.dob,
# MAGIC              tgt.forename = upd.forename,
# MAGIC              tgt.surname = upd.surname,
# MAGIC              tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED
# MAGIC   THEN INSERT (driverId, dob, forename,surname,createdDate ) VALUES (driverId, dob, forename,surname, current_timestamp)

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE f1_demo

# COMMAND ----------

delta_df = (spark.read.table("f1_demo.drivers_merge")
                 .select(col('driverId'),
                         col('dob'),
                         col('forename'),
                         col('surname'),
                         col('createdDate'),
                         col('updatedDate')
                 )
                )

# COMMAND ----------

delta_df.write.format("delta").mode("overwrite").save("/mnt/18formula1dl/demo/drivers_merge")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/18formula1dl/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/18formula1dl/demo/drivers_merge_test")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "dbfs:/user/hive/warehouse/mnt/18formula1dl/demo/drivers_merge_test")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId") \
  .whenMatchedUpdate(set = { "dob" : "upd.dob", "forename" : "upd.forename", "surname" : "upd.surname", "updatedDate": "current_timestamp()" } ) \
  .whenNotMatchedInsert(values =
    {
      "driverId": "upd.driverId",
      "dob": "upd.dob",
      "forename" : "upd.forename", 
      "surname" : "upd.surname", 
      "createdDate": "current_timestamp()"
    }
  ) \
  .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge_test;

# COMMAND ----------

display_df=spark.read.format("delta").load("/mnt/18formula1dl/demo/drivers_merge/")

# COMMAND ----------

display(drivers_day3_df)

# COMMAND ----------

display(display_df)

# COMMAND ----------

# MAGIC %md
# MAGIC History
# MAGIC Versioning
# MAGIC Time travel

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge TIMESTAMP AS OF '2022-10-03T20:13:07.000+0000';

# COMMAND ----------

EX_df=spark.read.format("delta").option("versionAsOf", 1).load("/mnt/18formula1dl/demo/drivers_merge")

# COMMAND ----------

display(EX_df)

# COMMAND ----------

EX2_df=spark.read.format("delta").option("versionAsOf", 1).load("dbfs:/user/hive/warehouse/mnt/18formula1dl/demo/drivers_merge")

# COMMAND ----------

EX2_df=spark.read.format("delta").load("/user/hive/warehouse/mnt/18formula1dl/demo/drivers_merge")

# COMMAND ----------

display(EX2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false;
# MAGIC VACUUM f1_demo.drivers_merge RETAIN 0 HOURS

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from f1_demo.drivers_merge where driverId=1;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge VERSION AS OF 9;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING f1_demo.drivers_merge VERSION AS OF 9 src
# MAGIC   ON (tgt.driverId=src.driverId)
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT * 

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction log

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge_txn(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE HISTORY f1_demo.drivers_merge_txn;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_processed.drivers_merge_delta(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS f1_test
# MAGIC LOCATION "/mnt/18formula1dl/test/"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_test.drivers_merge_delta_test(
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING, 
# MAGIC surname STRING,
# MAGIC createdDate DATE, 
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %fs ls /mnt/18formula1dl/demo/drivers_merge_test