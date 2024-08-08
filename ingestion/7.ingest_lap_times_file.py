# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap times file(multiple files)

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read csv file using dataframe reader

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("postion", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("milliseconds", StringType(), True)
                                    ])

# COMMAND ----------

lap_times_df = spark.read \
                .schema(lap_times_schema)\
                .csv(f"{raw_folder_path}/{v_file_date}/lap_times")

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename and add columns as required

# COMMAND ----------

lap_times_final_df = lap_times_df.withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write the data into parquet file

# COMMAND ----------

# overwrite_partition(lap_times_final_df,"f1_processed", "lap_times", "race_id")

# COMMAND ----------

merge_codition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.lap=src.lap"
merge_delta_data(lap_times_final_df, 'f1_processed','lap_times', processed_folder_path,merge_codition, 'race_id')

# COMMAND ----------

#lap_times_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.lap_times")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP table f1_processed.lap_times;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.lap_times group by race_id order by race_id DESC;