# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder(multiple files)

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

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                     StructField("raceId", IntegerType(), True),
                                     StructField("driverId", IntegerType(), True),
                                     StructField("constructorId", IntegerType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("position", IntegerType(), True),
                                     StructField("q1", StringType(), True),
                                     StructField("q2", StringType(), True),
                                     StructField("q3", StringType(), True)
                                    ])

# COMMAND ----------

qualifying_df = spark.read \
                .schema(qualifying_schema)\
                .option("multiLine", True)\
                .json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename and add columns as required

# COMMAND ----------

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyId", "qualify_id") \
                                            .withColumnRenamed("raceId", "race_id") \
                                            .withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Write the data into parquet file

# COMMAND ----------

# overwrite_partition(qualifying_final_df,"f1_processed", "qualifying", "race_id")

# COMMAND ----------

merge_codition="tgt.qualify_id=src.qualify_id AND tgt.race_id=src.race_id "
merge_delta_data(qualifying_final_df, 'f1_processed','qualifying', processed_folder_path,merge_codition, 'race_id')

# COMMAND ----------

#qualifying_final_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.qualifying")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP table f1_processed.qualifying;

# COMMAND ----------

# MAGIC %sql
# MAGIC select race_id, count(1) from f1_processed.qualifying group by race_id order by race_id DESC;