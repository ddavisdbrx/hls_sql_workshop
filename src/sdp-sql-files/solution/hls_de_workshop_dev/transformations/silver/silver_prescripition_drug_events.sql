--create insert-only table with all the business logic
CREATE STREAMING TABLE silver.prescription_drug_events_insert
AS
SELECT
    uuid() as prescription_drug_events_insert_key
   ,md5(pde.DESYNPUF_ID ||pde.SRVC_DT ||pde.PROD_SRVC_ID) as prescription_drug_events_key
  ,pde.DESYNPUF_ID as beneficiary_code
  ,cast(pde.PDE_ID as string) as ccw_part_d_event_number
  ,to_date(pde.SRVC_DT,'yyyyMMdd') as rx_service_date
  ,cast(pde.PROD_SRVC_ID as string) as product_service_id
  ,cast(pde.QTY_DSPNSD_NUM as double) as quantity_dispensed
  ,cast(pde.DAYS_SUPLY_NUM as int) as days_supply
  ,cast(pde.PTNT_PAY_AMT as double) as patient_pay_amount
  ,cast(pde.TOT_RX_CST_AMT as double) as gross_drug_cost
  ,current_timestamp as insert_timestamp
FROM stream(bronze.prescription_drug_events) pde;


--create the merged silver table
CREATE STREAMING TABLE silver.prescription_drug_events;

CREATE FLOW silver_prescription_drug_events AS AUTO CDC 
  INTO silver.prescription_drug_events
FROM
  stream(silver.prescription_drug_events_insert)
KEYS
  (prescription_drug_events_key)
SEQUENCE BY
  (insert_timestamp)
COLUMNS * EXCEPT
  (prescription_drug_events_insert_key)
STORED AS
  SCD TYPE 1;