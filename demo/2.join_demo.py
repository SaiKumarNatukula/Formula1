# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_folder_path}/races").filter("race_year = 2019") \
.withColumnRenamed("name", "race_name")

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_folder_path}/circuits") \
                        .filter("circuit_id < 70") \
                        .withColumnRenamed("name", "circuit_name")

# COMMAND ----------

display(races_df)

# COMMAND ----------

display(circuits_df)

# COMMAND ----------

race_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "inner")

# COMMAND ----------

display(race_circuits_df)

# COMMAND ----------

race_left_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "full")

# COMMAND ----------

display(race_left_circuits_df)

# COMMAND ----------

race_semi_circuits_df = circuits_df.join(races_df, circuits_df.circuit_id == races_df.circuit_id, "semi")

# COMMAND ----------

display(race_semi_circuits_df)

# COMMAND ----------

race_anti_circuits_df = races_df.join(circuits_df, circuits_df.circuit_id == races_df.circuit_id, "anti")

# COMMAND ----------

display(display(race_anti_circuits_df))

# COMMAND ----------

race_cross_df = races_df.crossJoin(circuits_df)

# COMMAND ----------

display(race_cross_df)

# COMMAND ----------

race_cross_df.count()

# COMMAND ----------

int(races_df.count()) * int(circuits_df.count())

# COMMAND ----------

