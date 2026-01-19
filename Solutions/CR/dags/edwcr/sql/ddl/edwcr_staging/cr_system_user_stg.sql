CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cr_system_user_stg (
securityid INT64
, userid STRING
, firstname STRING
, lastname STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
