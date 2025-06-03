-- Databricks notebook source
-- MAGIC %md
-- MAGIC #Bronze Load Notebook
-- MAGIC
-- MAGIC This notebook will load the data from the CSV files listed in our volumes into bronze tables. There are several concepts that you will see in each code block.
-- MAGIC
-- MAGIC [Volumes](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-volumes)
-- MAGIC Our source CSV files are stored in volumes. This is a component of Unity Catalog where we can store and managed unstrutured data.
-- MAGIC
-- MAGIC [Streaming Table](https://docs.databricks.com/aws/en/dlt-ref/dlt-sql-ref-create-streaming-table)
-- MAGIC Streaming tables are used to incrementally load data. In this case, we will be inserting all new data into our bronze tables.
-- MAGIC
-- MAGIC [read_files](https://learn.microsoft.com/en-us/azure/databricks/sql/language-manual/functions/read_files)
-- MAGIC This will use Auto Loader to identify new files stored in a volume and then stream that data into the streaming table
-- MAGIC
-- MAGIC <strong>inferColumnTypes</strong>
-- MAGIC In our architecture, we made the decision that we want all data loaded from the CSVs to land as strings in Bronze. This is the most accepting file type as the data that comes in may be in predictable. By default, "load_files" will attempt to identify the data types for the load. This option will allow us to turn off that default. Note that Auto Loader does not have this turned on by default unless it's being used as part of the "load_files" Syntax in SQL. Please see the documentation for a [full list of Auto Loader Options](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/options).
-- MAGIC
-- MAGIC <strong>${volume_path}</strong>
-- MAGIC This is a variable that will be passed in from the [DLT pipeline](https://docs.databricks.com/aws/en/dlt/configure-pipeline). We are parameterizing this as a variable as we'll want to dynamically change the source of the files as we move the pipeline to new enviornments (dev, test, prod, etc.)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.beneficiary
  COMMENT "raw data for summaries of beneficiaries"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/beneficiary/*",
  format => 'csv',
  inferColumnTypes => false
)


-- COMMAND ----------

CREATE STREAMING TABLE bronze.carrier_claims
  COMMENT "raw data for carrier claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/carrier_claims/*",
  format => 'csv',
  inferColumnTypes => false
)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.inpatient_claims
  COMMENT "raw data for inpatient claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/inpatient_claims/*",
  format => 'csv',
  inferColumnTypes => false
)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.outpatient_claims
  COMMENT "raw data for outpatient claim transactions"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/outpatient_claims/*",
  format => 'csv',
  inferColumnTypes => false
)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.prescription_drug_events
  COMMENT "raw data for prescription drug events"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/prescription_drug_events/*",
  format => 'csv',
  inferColumnTypes => false
)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.icd_codes
  COMMENT "Lookups for icd9 codes"
AS 
SELECT
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/icd_codes/*",
  format => 'csv',
  inferColumnTypes => false
)

-- COMMAND ----------

CREATE STREAMING TABLE bronze.npi_codes
  COMMENT "Lookups for National Provider Identifier Number"
AS 
SELECT
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/npi_codes/*",
  format => 'csv',
  inferColumnTypes => false
)


-- COMMAND ----------

CREATE STREAMING TABLE bronze.lookups
  COMMENT "Code lookups accross tables"
AS 
SELECT 
  * 
  ,current_timestamp as insert_timestamp
  ,_metadata
FROM STREAM read_files(
  "${volume_path}/lookups/*",
  format => 'csv',
  inferColumnTypes => false
)

