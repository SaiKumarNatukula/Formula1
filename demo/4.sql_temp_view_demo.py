# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Local temp view

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from v_race_results where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"select * from v_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Global temp view

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW tables IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from global_temp.gv_race_results

# COMMAND ----------

gvrace_results_2019_df = spark.sql(f"select * from global_temp.gv_race_results where race_year = {p_race_year}")

# COMMAND ----------

display(gvrace_results_2019_df)

# COMMAND ----------

