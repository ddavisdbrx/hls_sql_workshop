/*****************************************************************************************
We will build the Fact Tables as materilized views.

We are using these instead of streaming tables as it's possible that data may be updated 
or deleted and we want to refelect that easily in the fact tables. We will be relying on 
Enzyme to find the most performative way to update these tables.
******************************************************************************************/

CREATE MATERIALIZED VIEW  gold.fact_prescription_drug_events
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