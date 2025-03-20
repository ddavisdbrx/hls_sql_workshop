-- Databricks notebook source
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

