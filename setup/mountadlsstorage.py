# Databricks notebook source
storage_account_name= "18formula1dl"
client_id= dbutils.secrets.get("formula1-scope","databricks-app-client-id" )
tenant_id= dbutils.secrets.get("formula1-scope","databricks-app-tenant-id" )
client_secret= dbutils.secrets.get("formula1-scope","databricks-app-client-secret" )

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": f"{client_id}",
           "fs.azure.account.oauth2.client.secret": f"{client_secret}",
           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

def mountadls(container_name):
  dbutils.fs.mount(
  source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
  mount_point = f"/mnt/{storage_account_name}/{container_name}",
  extra_configs = configs)

# COMMAND ----------

mountadls("processed")

# COMMAND ----------

mountadls("raw")

# COMMAND ----------

mountadls("presentation")

# COMMAND ----------

mountadls("demo")

# COMMAND ----------

mountadls("test")

# COMMAND ----------

dbutils.fs.ls("mnt/18formula1dl/processed")


# COMMAND ----------

dbutils.fs.ls("mnt/18formula1dl/raw")

# COMMAND ----------

dbutils.fs.ls("mnt/18formula1dl/presentation")

# COMMAND ----------

dbutils.fs.ls("mnt/18formula1dl/demo")
