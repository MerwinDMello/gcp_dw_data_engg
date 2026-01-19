CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_lookup_code_stg (
lookup_id INT64 NOT NULL
, group_id INT64
, lookup_code STRING NOT NULL
, lookup_sub_code STRING
, lookup_desc STRING
, id INT64
, lookup_name STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
