CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_imaging_mode_stg (
imaging_mode_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
