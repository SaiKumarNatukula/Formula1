# Databricks notebook source
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest drivers.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col
from pyspark.sql.types import StructType, IntegerType, StructField, StringType, DateType

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read json file using dataframe reader

# COMMAND ----------

name_schema= StructType(fields=[StructField("forename", StringType(), True),
                                     StructField("surname", StringType(), True)
                                    ])

# COMMAND ----------

drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                     StructField("driverRef", StringType(), True),
                                     StructField("number", IntegerType(), True),
                                     StructField("code", StringType(), True),
                                     StructField("name", name_schema),
                                     StructField("dob", DateType(), True),
                                     StructField("nationality", StringType(), True),
                                     StructField("url", StringType(), True)
                                    ])

# COMMAND ----------

drivers_df = spark.read \
                .schema(drivers_schema)\
                .json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename and add the neccessary columns

# COMMAND ----------

driver_withcolumns_df = drivers_df.withColumnRenamed("driverId", "driver_id") \
                                            .withColumnRenamed("driverRef", "driver_ref") \
                                            .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname"))) \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("ingestion_date", current_timestamp()) \
                                            .withColumn("file_date", lit(v_file_date))
                                            

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Drop the unwanted column

# COMMAND ----------

drivers_final_df = driver_withcolumns_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write the data to parquet file

# COMMAND ----------

drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.drivers")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from f1_processed.drivers;

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

