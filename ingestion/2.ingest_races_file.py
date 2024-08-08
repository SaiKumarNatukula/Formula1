# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC ### ingest races.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Import statements

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, DateType
from pyspark.sql.functions import col, current_timestamp, lit, to_timestamp, concat

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Define schema 

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

# MAGIC %md
# MAGIC #####Step 2: Read the csv file using spark dataframe reader

# COMMAND ----------

races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv(f"{raw_folder_path}/{v_file_date}/races.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Add the required columns

# COMMAND ----------

races_add_df= races_df.withColumn("race_timestamp", \
                                to_timestamp(concat(col("date"), lit(" "), col("time")), 'yyyy-MM-dd HH:mm:ss')) \
                                .withColumn("data_source", lit(v_data_source)) \
                                .withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("file_date", lit(v_file_date))
                                

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Select and rename the required columns as required

# COMMAND ----------

races_selected_df= races_add_df.select(col("raceId").alias("race_id"), col("year").alias("race_year"), col("round"), col("circuitId").alias("circuit_id"), col("name"), col("race_timestamp"), col("data_source"), col("ingestion_date"), col("file_date"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5: Write the data to parquet file

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_processed.races;

# COMMAND ----------

dbutils.notebook.exit("Success")