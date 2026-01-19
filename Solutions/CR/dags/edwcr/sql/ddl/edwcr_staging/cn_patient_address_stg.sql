CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_patient_address_stg (
nav_patient_id NUMERIC(18,0) NOT NULL
, address_line_1_text STRING
, address_line_2_text STRING
, city_name STRING
, state_code STRING
, zip_code STRING
, housing STRING
, localhousingaddress STRING
, otherlocalhousingaddress STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
