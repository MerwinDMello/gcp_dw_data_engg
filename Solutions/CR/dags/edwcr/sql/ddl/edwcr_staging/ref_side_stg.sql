CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_side_stg (
side_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
