# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import count, sum, countDistinct, col, desc, rank, current_timestamp, when

# COMMAND ----------

# MAGIC %md
# MAGIC find the years for which data needs to be reprocessed

# COMMAND ----------

race_results_list = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                        .filter(f"file_date = '{v_file_date}'") \
                        .select("race_year") \
                        .distinct() \
                        .collect()

# COMMAND ----------

race_results_list

# COMMAND ----------

race_year_list = []
for race_year in race_results_list:
    race_year_list.append(race_year.race_year)

# COMMAND ----------

race_results_df = spark.read.format("delta").load(f"{presentation_folder_path}/race_results") \
                            .filter(col("race_year").isin(race_year_list))

# COMMAND ----------

driver_standings_df = race_results_df.groupBy("race_year", "driver_name", "driver_nationality") \
                                     .agg(sum("points").alias("total_points"),
                                         count(when(col("position")==1, True)).alias("wins"))

# COMMAND ----------

from pyspark.sql.window import Window
driverRankSpec = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins")) 

# COMMAND ----------

final_df = driver_standings_df.withColumn("rank", rank().over(driverRankSpec))

# COMMAND ----------

merge_codition="tgt.driver_name=src.driver_name AND tgt.race_year=src.race_year"
merge_delta_data(final_df, 'f1_presentation','driver_standings', presentation_folder_path,merge_codition, 'race_year')

# COMMAND ----------

# overwrite_partition(final_df,"f1_presentation", "driver_standings", "race_year")

# COMMAND ----------

#final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_presentation.driver_standings")

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_year, count(1) from f1_presentation.driver_standings group by race_year order by race_year DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_presentation.driver_standings where race_year=2021;