CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_patient_phone_num_stg (
patient_id INT64 NOT NULL
, contact_id INT64 NOT NULL
, phone_num_type_code STRING NOT NULL
, phone_num STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
