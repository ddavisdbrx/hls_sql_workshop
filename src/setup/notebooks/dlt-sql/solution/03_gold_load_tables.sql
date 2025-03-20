-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###dim_beneficiary

-- COMMAND ----------

CREATE STREAMING LIVE VIEW vw_dim_beneficiary
AS
select 
   uuid() as beneficiary_key
  ,* 
from stream(silver.beneficiary_insert)

-- COMMAND ----------

CREATE OR REFRESH STREAMING TABLE gold.dim_beneficiary;

APPLY CHANGES INTO
  gold.dim_beneficiary
FROM
  stream(vw_dim_beneficiary)
KEYS
  (beneficiary_code)
SEQUENCE BY
  (year)
COLUMNS * EXCEPT
  (beneficiary_insert_key,year, insert_timestamp)
STORED AS
  SCD TYPE 2
TRACK HISTORY ON * EXCEPT 
  (year);

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_icd_code

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW gold.dim_diagnosis
AS
SELECT 
   icd_codes_key as diagnosis_key
  ,diagnosis_code
  ,diagnosis_long_description
  ,diagnosis_short_description
FROM silver.icd_codes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_provider

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW  gold.dim_provider
AS
SELECT 
    npi_codes_key as provider_key
   ,npi_code
   ,entity_type
   ,provider_organization_name
   ,provider_other_organization_name
   ,provider_first_line_business_mailing_address
   ,provider_second_line_business_mailing_address
   ,provider_business_mailing_address_city_name
   ,provider_business_mailing_address_state_name
   ,provider_business_mailing_address_postal_code
   ,provider_business_mailing_address_code_if_outside_us
   ,provider_first_line_business_practice_location
   ,provider_second_line_business_practice_location
   ,provider_business_practice_location_address_city_name
   ,provider_business_practice_location_address_state_name
   ,provider_business_practice_location_address_postal_code
   ,provider_business_practice_location_address_country_code_if_outside_us
FROM silver.npi_codes

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###dim_date

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW  gold.dim_date
AS
SELECT
  date
  ,date_num
  ,year
  ,year_month
  ,calendar_quarter
  ,month_num
  ,month_name
  ,month_short_name
  ,week_num
  ,day_num_of_year
  ,day_num_of_month
  ,day_num_of_week
  ,day_name
  ,day_short_name
  ,quarter
  ,year_quarter_num
  ,day_num_of_quarter
FROM read_files('${volume_path}/date/DimDate.csv',
  format => 'csv',
  header => true)
where year in (2008,2009,2010)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_prescription_drug_events

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW  gold.fact_prescription_drug_events
AS
SELECT
   prescription_drug_events_key
  ,ccw_part_d_event_number
  ,db.beneficiary_key
  ,rx_service_date
  ,product_service_id
  ,quantity_dispensed
  ,days_supply
  ,patient_pay_amount
  ,gross_drug_cost
FROM silver.prescription_drug_events p
LEFT JOIN gold.dim_beneficiary db on p.beneficiary_code = db.beneficiary_code 
  AND year(p.rx_service_date) >= db.__START_AT
  AND year(p.rx_service_date) < coalesce(db.__END_AT,9999)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_patient_claims

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW  gold.fact_patient_claims
AS
SELECT
   c.patient_claims_key
  ,c.claim_id
  ,db.beneficiary_key
  ,claim_type
  ,md5(c.attending_physician_npi) as attending_physician_provider_key
  ,md5(c.operating_physician_npi) as operating_physician_provider_key
  ,md5(c.other_physician_npi) as other_physician_provider_key
  ,c.claim_line_segment
  ,c.claim_start_date
  ,c.claim_end_date
  ,c.inpatient_admission_date
  ,c.claim_payment_amount
  ,c.primary_payer_claim_paid_amount
  ,md5(c.icd9_diagnosis_code_1) as diagnosis_key_1
  ,md5(c.icd9_diagnosis_code_2) as diagnosis_key_2
  ,md5(c.icd9_diagnosis_code_3) as diagnosis_key_3
  ,md5(c.icd9_diagnosis_code_4) as diagnosis_key_4
  ,md5(c.icd9_diagnosis_code_5) as diagnosis_key_5
  ,md5(c.icd9_procedure_code_1) as procedure_key_1
  ,md5(c.icd9_procedure_code_2) as procedure_key_2
  ,md5(c.icd9_procedure_code_3) as procedure_key_3
  ,md5(c.icd9_procedure_code_4) as procedure_key_4
  ,md5(c.icd9_procedure_code_5) as procedure_key_5
  ,md5(c.icd9_admitting_diagnosis_code) as admitting_key
FROM silver.patient_claims c
LEFT JOIN gold.dim_beneficiary db on c.beneficiary_code = db.beneficiary_code 
  AND year(c.claim_start_date) >= db.__START_AT
  AND year(c.claim_start_date) < coalesce(db.__END_AT,9999)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###fact_carrier_claims

-- COMMAND ----------

CREATE OR REFRESH MATERIALIZED VIEW  gold.fact_carrier_claims
--TBLPROPERTIES ("pipelines.autoOptimize.zOrderCols"="claim_start_date,beneficiary_key")
CLUSTER BY (beneficiary_key,claim_start_date)
AS
SELECT
   carrier_claims_key
  ,cc.claim_id
  ,db.beneficiary_key
  ,cc.claim_start_date
  ,cc.claim_end_date
  ,md5(line_icd9_diagnosis_code) as line_diagnosis_key
  ,md5(claim_diagnosis_code_1) as diagnosis_key_1
  ,md5(claim_diagnosis_code_2) as diagnosis_key_2
  ,md5(claim_diagnosis_code_3) as diagnosis_key_3
  ,md5(claim_diagnosis_code_4) as diagnosis_key_4
  ,md5(claim_diagnosis_code_5) as diagnosis_key_5
  ,md5(claim_diagnosis_code_6) as diagnosis_key_6
  ,md5(claim_diagnosis_code_7) as diagnosis_key_7
  ,md5(claim_diagnosis_code_8) as diagnosis_key_8
  ,md5(provider_physician_npi_1) as provider_key_1
  ,md5(provider_physician_npi_2) as provider_key_2
  ,md5(provider_physician_npi_3) as provider_key_3
  ,md5(provider_physician_npi_4) as provider_key_4
  ,md5(provider_physician_npi_5) as provider_key_5
  ,cast(cc.claim_end_date - cc.claim_start_date as int) + 1 as claim_days
  ,cast(line_number as int) as line_number
  ,nch_payment_amount
  ,line_beneficiary_part_b_deductable_amount
  ,line_beneficiary_primary_payer_paid_amount
  ,line_coinsurance_amount
  ,line_allowed_charge_amount
  ,line_processing_indicator_code
FROM silver.carrier_claims cc 
LEFT JOIN gold.dim_beneficiary db on cc.beneficiary_code = db.beneficiary_code 
  AND year(cc.claim_start_date) >= db.__START_AT
  AND year(cc.claim_start_date) < coalesce(db.__END_AT,9999)  
UNPIVOT ((nch_payment_amount,line_beneficiary_part_b_deductable_amount,line_beneficiary_primary_payer_paid_amount,line_coinsurance_amount,line_allowed_charge_amount,line_processing_indicator_code,line_icd9_diagnosis_code)
  FOR line_number in ((nch_payment_amount_1,line_beneficiary_part_b_deductable_amount_1,line_beneficiary_primary_payer_paid_amount_1,line_coinsurance_amount_1,line_allowed_charge_amount_1,line_processing_indicator_code_1,line_icd9_diagnosis_code_1) as `1`
                     ,(nch_payment_amount_2,line_beneficiary_part_b_deductable_amount_2,line_beneficiary_primary_payer_paid_amount_2,line_coinsurance_amount_2,line_allowed_charge_amount_2,line_processing_indicator_code_2,line_icd9_diagnosis_code_2) as `2`
                     ,(nch_payment_amount_3,line_beneficiary_part_b_deductable_amount_3,line_beneficiary_primary_payer_paid_amount_3,line_coinsurance_amount_3,line_allowed_charge_amount_3,line_processing_indicator_code_3,line_icd9_diagnosis_code_3) as `3`
                     ,(nch_payment_amount_4,line_beneficiary_part_b_deductable_amount_4,line_beneficiary_primary_payer_paid_amount_4,line_coinsurance_amount_4,line_allowed_charge_amount_4,line_processing_indicator_code_4,line_icd9_diagnosis_code_4) as `4`
                     ,(nch_payment_amount_5,line_beneficiary_part_b_deductable_amount_5,line_beneficiary_primary_payer_paid_amount_5,line_coinsurance_amount_5,line_allowed_charge_amount_5,line_processing_indicator_code_5,line_icd9_diagnosis_code_5) as `5`
                     ,(nch_payment_amount_6,line_beneficiary_part_b_deductable_amount_6,line_beneficiary_primary_payer_paid_amount_6,line_coinsurance_amount_6,line_allowed_charge_amount_6,line_processing_indicator_code_6,line_icd9_diagnosis_code_6) as `6`
                     ,(nch_payment_amount_7,line_beneficiary_part_b_deductable_amount_7,line_beneficiary_primary_payer_paid_amount_7,line_coinsurance_amount_7,line_allowed_charge_amount_7,line_processing_indicator_code_7,line_icd9_diagnosis_code_7) as `7`
                     ,(nch_payment_amount_8,line_beneficiary_part_b_deductable_amount_8,line_beneficiary_primary_payer_paid_amount_8,line_coinsurance_amount_8,line_allowed_charge_amount_8,line_processing_indicator_code_8,line_icd9_diagnosis_code_8) as `8`
                     ,(nch_payment_amount_9,line_beneficiary_part_b_deductable_amount_9,line_beneficiary_primary_payer_paid_amount_9,line_coinsurance_amount_9,line_allowed_charge_amount_9,line_processing_indicator_code_9,line_icd9_diagnosis_code_9) as `9`
                     ,(nch_payment_amount_10,line_beneficiary_part_b_deductable_amount_10,line_beneficiary_primary_payer_paid_amount_10,line_coinsurance_amount_10,line_allowed_charge_amount_10,line_processing_indicator_code_10,line_icd9_diagnosis_code_10) as `10`
                     ,(nch_payment_amount_11,line_beneficiary_part_b_deductable_amount_11,line_beneficiary_primary_payer_paid_amount_11,line_coinsurance_amount_11,line_allowed_charge_amount_11,line_processing_indicator_code_11,line_icd9_diagnosis_code_11) as `11`
                     ,(nch_payment_amount_12,line_beneficiary_part_b_deductable_amount_12,line_beneficiary_primary_payer_paid_amount_12,line_coinsurance_amount_12,line_allowed_charge_amount_12,line_processing_indicator_code_12,line_icd9_diagnosis_code_12) as `12`
                     ,(nch_payment_amount_13,line_beneficiary_part_b_deductable_amount_13,line_beneficiary_primary_payer_paid_amount_13,line_coinsurance_amount_13,line_allowed_charge_amount_13,line_processing_indicator_code_13,line_icd9_diagnosis_code_13) as `13`
                     ))
WHERE (nch_payment_amount <> 0 
     OR line_beneficiary_part_b_deductable_amount <> 0
     OR line_beneficiary_primary_payer_paid_amount <> 0
     OR line_coinsurance_amount <> 0
     OR line_allowed_charge_amount <> 0
     OR line_processing_indicator_code <> 0
     OR line_icd9_diagnosis_code <> 0
      )

-- COMMAND ----------

CREATE LIVE TABLE gold.rpt_patient_claims
AS
select
   b.*
  ,p.*
  ,c.claim_id
  ,c.claim_type
  ,c.claim_start_date
  ,c.claim_end_date
  ,c.inpatient_admission_date
  ,c.claim_payment_amount
  ,c.primary_payer_claim_paid_amount
from gold.fact_patient_claims c
join gold.dim_beneficiary b on c.beneficiary_key = b.beneficiary_key
join gold.dim_provider p on c.attending_physician_provider_key = p.provider_key


-- COMMAND ----------

/*
CREATE LIVE TABLE gold.my_fk_test
  (
     beneficiary_key STRING
    ,gross_drug_cost DOUBLE
    ,CONSTRAINT myfktest_beneficiary_fk FOREIGN KEY (beneficiary_key) REFERENCES gold.dim_beneficiary
  )
AS
SELECT
   beneficiary_key
  ,gross_drug_cost
FROM gold.fact_prescription_drug_events
*/
