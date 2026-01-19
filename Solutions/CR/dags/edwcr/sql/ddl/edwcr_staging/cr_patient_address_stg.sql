CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_address_stg (
cr_patient_id INT64 NOT NULL
, state_id STRING
, address_line_1_text STRING
, address_line_2_text STRING
, city_name STRING
, zip_code STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
