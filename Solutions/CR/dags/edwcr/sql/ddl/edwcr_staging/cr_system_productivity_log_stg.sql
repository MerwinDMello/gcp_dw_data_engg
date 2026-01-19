CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_system_productivity_log_stg (
system_productivity_log_id INT64 NOT NULL
, cr_patient_id INT64
, tumor_id INT64
, system_user_id_code STRING
, system_change_status_date INT64
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
