# Databricks notebook source
# MAGIC %run "../includes/configurations"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ingest constructors.json file

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read json file using dataframe reader

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructors_df = spark.read \
                .schema(constructors_schema)\
                .json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Drop the unwanted column

# COMMAND ----------

constructors_dropped_df = constructors_df.drop("url")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Rename and add the neccessary columns

# COMMAND ----------

constructors_final_df = constructors_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                            .withColumnRenamed("constructorRef", "constructor_ref") \
                                            .withColumn("data_source", lit(v_data_source)) \
                                            .withColumn("ingestion_date", current_timestamp())\
                                            .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write the data to parquet file

# COMMAND ----------

constructors_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1) from f1_processed.constructors;

# COMMAND ----------

dbutils.notebook.exit("Success")