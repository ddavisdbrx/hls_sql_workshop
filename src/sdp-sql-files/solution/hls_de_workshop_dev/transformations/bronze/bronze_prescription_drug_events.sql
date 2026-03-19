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
);