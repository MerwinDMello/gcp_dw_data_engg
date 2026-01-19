CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_hospital_stg (
hospital_id INT64
, hospital_code STRING
, hospital_name STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
