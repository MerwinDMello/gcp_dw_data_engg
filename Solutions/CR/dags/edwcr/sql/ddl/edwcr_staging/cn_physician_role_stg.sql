CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_physician_role_stg (
physician_id INT64 NOT NULL
, physician_role_code STRING NOT NULL
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
