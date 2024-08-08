# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pitstops.json file(multiline)

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
# MAGIC ##### Step 1: Read json file using dataframe reader

# COMMAND ----------

pitstops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("stop", StringType(), True),
                                     StructField("lap", IntegerType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("duration", StringType(), True),
                                     StructField("milliseconds", StringType(), True)
                                    ])

# COMMAND ----------

pitstops_df = spark.read \
                .schema(pitstops_schema)\
                .option("multiLine", True)\
                .json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename and add columns as required

# COMMAND ----------

pitstops_final_df = pitstops_df.withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write the data into parquet file

# COMMAND ----------

# overwrite_partition(pitstops_final_df,"f1_processed", "pit_stops","race_id")

# COMMAND ----------

merge_codition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.stop=src.stop"
merge_delta_data(pitstops_final_df, 'f1_processed','pit_stops', processed_folder_path,merge_codition, 'race_id')

# COMMAND ----------

#pitstops_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.pit_stops")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct race_id from f1_processed.pit_stops order by race_id DESC;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP table f1_processed.pit_stops;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.pit_stops group by race_id order by race_id DESC;