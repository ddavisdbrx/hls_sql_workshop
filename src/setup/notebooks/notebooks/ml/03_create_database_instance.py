# Databricks notebook source
dbutils.widgets.text('catalog','hls_sql_workshop')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.76.0

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import DatabaseInstance

w = WorkspaceClient()

# ignore "already exists" error
try:
 instance = w.database.create_database_instance(
    DatabaseInstance(
        name = f"hls-sql-workshop",
        capacity="CU_1"
    )
  )
 print(f'Successfully created database instance called hls_sql_workshop')
except Exception as e:
  error_msg = str(e).lower()
  if "already exists" in error_msg or "instance name is not unique" in error_msg:
    pass
  else:
    raise e

# COMMAND ----------

import time

print("Waiting 5 minutes for the database instance to be provisioned...")
time.sleep(5 * 60)
