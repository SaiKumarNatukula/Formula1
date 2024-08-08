-- Databricks notebook source
-- MAGIC %md
-- MAGIC ##### Databases

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Demo;

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Demo;

-- COMMAND ----------

show databases

-- COMMAND ----------

describe database demo;

-- COMMAND ----------

describe database extended demo;

-- COMMAND ----------

select current_database();

-- COMMAND ----------

show tables;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use demo;


-- COMMAND ----------

select current_database()

-- COMMAND ----------

show tables;

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##### Tables

-- COMMAND ----------

-- MAGIC %run "../includes/configurations"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").saveAsTable("demo.race_results_python")

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe race_results_python;

-- COMMAND ----------

describe extended race_results_python

-- COMMAND ----------

select * from demo.race_results_python where race_year=2020;

-- COMMAND ----------

CREATE TABLE demo.race_results_sql
AS 
select * from demo.race_results_python where race_year=2020;

-- COMMAND ----------

select current_database()

-- COMMAND ----------

select * from demo.race_results_sql;

-- COMMAND ----------

describe extended demo.race_results_sql;

-- COMMAND ----------

drop table demo.race_results_sql;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### External table

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format("parquet").option("path", f"{presentation_folder_path}/race_results_ext_py").saveAsTable("demo.race_results_ext_py")

-- COMMAND ----------

describe extended demo.race_results_ext_py

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql
(race_year INT,
race_name STRING,
race_date TIMESTAMP,
circuit_location STRING,
driver_name STRING,
driver_number INT,
driver_nationality STRING,
team STRING,
grid INT,
fastest_lap INT,
race_time STRING,
points FLOAT,
position INT,
created_date TIMESTAMP
)
USING parquet
LOCATION "/mnt/108formula1dl/presentation/race_results_ext_sql"

-- COMMAND ----------

SHOW TABLES IN demo;

-- COMMAND ----------

INSERT INTO demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_py WHERE race_year = 2020;

-- COMMAND ----------

select * from demo.race_results_ext_sql

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### Views

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2012;

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS
SELECT *
  FROM demo.race_results_python
 WHERE race_year = 2000;

-- COMMAND ----------

show tables in demo

-- COMMAND ----------

use f1_processed

-- COMMAND ----------

select nationality, name, dob, rank() over(partition by nationality order by dob DESC) as age_rank
from drivers;

-- COMMAND ----------

