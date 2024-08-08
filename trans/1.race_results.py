# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-23")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Read from all the required tables

# COMMAND ----------

races_df = spark.read.format("delta").load(f"{processed_folder_path}/races") \
                        .withColumnRenamed("name", "race_name") \
                        .withColumnRenamed("race_timestamp", "race_date") 

# COMMAND ----------

circuits_df = spark.read.format("delta").load(f"{processed_folder_path}/circuits")\
                        .withColumnRenamed("location", "circuit_location") 

# COMMAND ----------

drivers_df = spark.read.format("delta").load(f"{processed_folder_path}/drivers") \
                       .withColumnRenamed("number", "driver_number") \
                       .withColumnRenamed("name", "driver_name") \
                       .withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

constructors_df = spark.read.format("delta").load(f"{processed_folder_path}/constructors") \
                        .withColumnRenamed("name", "team") 

# COMMAND ----------

results_df = spark.read.format("delta").load(f"{processed_folder_path}/results") \
                        .filter(f"file_date = '{v_file_date}'") \
                        .withColumnRenamed("time", "race_time") \
                        .withColumnRenamed("race_id", "results_race_id") \
                        .withColumnRenamed("file_date", "results_file_date")

# COMMAND ----------

display(results_df)

# COMMAND ----------

race_circuits_df=races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "inner") \
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Join all other dataframes

# COMMAND ----------

race_results_df = results_df.join(race_circuits_df, race_circuits_df.race_id == results_df.results_race_id)  \
                            .join(constructors_df, constructors_df.constructor_id == results_df.constructor_id) \
                            .join(drivers_df, drivers_df.driver_id == results_df.driver_id) 
                            

# COMMAND ----------

final_df = race_results_df.select("race_id", "race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position", "results_file_date") \
.withColumn("created_date", current_timestamp()) \
.withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

merge_codition="tgt.driver_name=src.driver_name AND tgt.race_id=src.race_id"
merge_delta_data(final_df, 'f1_presentation','race_results', presentation_folder_path,merge_codition, 'race_id')

# COMMAND ----------

# overwrite_partition(final_df,"f1_presentation", "race_results", "race_id")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_presentation.race_results 
# MAGIC group by race_id 
# MAGIC order by race_id DESC

# COMMAND ----------

