# Databricks notebook source
from pyspark.sql.functions import count, sum, countDistinct, col, desc, rank, current_timestamp

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Aggregations

# COMMAND ----------

demo_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                            .filter("race_year=2020")

# COMMAND ----------

display(demo_df)

# COMMAND ----------

demo_df.select(count("race_name")).show()

# COMMAND ----------

demo_df.select(countDistinct("race_name")).show()

# COMMAND ----------

demo_df.filter("driver_name='Lewis Hamilton'").select(sum("points"), countDistinct("race_name"))\
        .withColumnRenamed("sum(points)", "Total points")\
        .withColumnRenamed("count(DISTINCT race_name)", "Total races") \
        .show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Group By

# COMMAND ----------

demo_df.groupBy("driver_name") \
.agg(sum("points").alias("Total points"), count("race_name").alias("Total number of races")) \
.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Window functions

# COMMAND ----------

demo2_df = spark.read.parquet(f"{presentation_folder_path}/race_results") \
                            .filter("race_year in (2019,2020)")

# COMMAND ----------

display(demo2_df)

# COMMAND ----------

demo_grouped_df = demo2_df.groupBy("race_year", "driver_name") \
.agg(sum("points").alias("total_points"), count("race_name").alias("total_races")) 

# COMMAND ----------

from pyspark.sql.window import Window
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points")) 

# COMMAND ----------

display(demo_grouped_df)

# COMMAND ----------

demo_grouped_df.withColumn("rank", rank().over(driverRankSpec)).show(100)

# COMMAND ----------

