CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_tumor_type_stg (
tumor_type_id INT64 NOT NULL
, tumor_type_desc STRING
, navigation_tumor_code_id INT64
, tumor_type_group_name STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
