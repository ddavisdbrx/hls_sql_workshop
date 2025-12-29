# Databricks notebook source
dbutils.widgets.text('catalog','hls_sql_workshop')
catalog = dbutils.widgets.get('catalog')
print(f'catalog = {catalog}')

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.76.0

# COMMAND ----------

# MAGIC %pip install mlflow typing_extensions==4.4.0

# COMMAND ----------

# DBTITLE 1,get latest model version
# from mlflow.tracking import MlflowClient
# model_version_infos = MlflowClient().search_model_versions(f"name = '{catalog}.ai.predict_claims_amount_model'")
# latest_model_version = max([int(model_version_info.version) for model_version_info in model_version_infos])
# print(f'Latest model version: {latest_model_version}')

from mlflow.tracking import MlflowClient

# Initialize the MLflow client
client = MlflowClient()

# Define the model name and alias
model_name = f'{catalog}.ai.predict_claims_amount_model'
alias = "production"

# Get the model version information using the alias
model_version_info = client.get_model_version_by_alias(model_name, alias)

# Extract the version number
model_version = model_version_info.version

print(f'Model name: {model_name} \nModel version for alias "{alias}": {model_version}')

# COMMAND ----------

# DBTITLE 1,create synced table
# create synced table

import time
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.database import (
    SyncedDatabaseTable,
    SyncedTableSpec,
    NewPipelineSpec,
    SyncedTableSchedulingPolicy
)

# add a wait time when the database instance is still being provisioned
max_wait_minutes = 20
wait_interval_seconds = 60
attempts = 0

w = WorkspaceClient()

while attempts < max_wait_minutes:
  try:
    synced_table = w.database.create_synced_database_table(
        SyncedDatabaseTable(
            name=f"{catalog}.ai.feature_beneficiary_synced_new",
            database_instance_name="hls-sql-workshop",
            logical_database_name=f"{catalog}",
            spec=SyncedTableSpec(
                source_table_full_name=f"{catalog}.ai.feature_beneficiary",
                primary_key_columns=["beneficiary_code"],
                scheduling_policy=SyncedTableSchedulingPolicy.SNAPSHOT,
                create_database_objects_if_missing=True,
                new_pipeline_spec=NewPipelineSpec(
                    storage_catalog=f"{catalog}",
                    storage_schema="ai"
                )
            ),
        )
    )
    print(f"Created synced table: {synced_table.name}")
    break
  except Exception as e:
      error_msg = str(e).lower()
      if "already exists" in error_msg:
          print(f"Synced table name already exists: {catalog}.ai.feature_beneficiary_synced")
          break
      elif "starting state" in error_msg:
          attempts += 1
          print(f"{e} \nWaiting 1 minute before retrying (max 15 minutes)... \nAttempt: ({attempts}/{max_wait_minutes})")
          time.sleep(wait_interval_seconds)
      else:
          raise e

# COMMAND ----------

from mlflow.deployments import get_deploy_client

client = get_deploy_client("databricks")

# ignore "already exists" error
try:
    endpoint = client.create_endpoint(
        name="predict_claims_amount",
        config={
            "served_entities": [
                {
                    "name": "predict_claims_amount_entity_test",
                    "entity_name": f"{catalog}.ai.predict_claims_amount_model",
                    "entity_version": f"{model_version}",
                    "workload_size": "Small",
                    "scale_to_zero_enabled": True
                }
            ],
            "ai_gateway": {
                "usage_tracking_config": {
                "enabled": True
                    },
                "inference_table_config": {
                "catalog_name": f"{catalog}",
                "schema_name": "ai",
                "enabled": True
                    }
            },
            "tags": [
                {
                "key": "project",
                "value": "hls_sql_workshop"
                }
        ]
        }
    )
except Exception as e:
 if "already exists" in str(e):
   pass
 else:
   raise e
