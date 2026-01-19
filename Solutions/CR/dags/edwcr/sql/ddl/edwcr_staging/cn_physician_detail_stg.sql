CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.cn_physician_detail_stg (
physician_id INT64
, physician_name STRING
, physician_phone_num STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
