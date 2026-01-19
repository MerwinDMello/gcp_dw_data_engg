CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_medical_oncology_stg (
cycle_id INT64
, treatment_id INT64
, drug_route_id STRING
, drug_dose_unit_id STRING
, drug_id INT64
, drug_days_given_num_text STRING
, drug_frequency_num NUMERIC
, drug_hospital_id STRING
, nsc_id STRING
, nsc_subcode STRING
, treatment_start_date DATE
, treatment_end_date DATE
, cycle_num_text STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
, total_drug_dose_amt NUMERIC(12,2)
)
  ;
