CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_cr_treatment_type_stg (
treatment_type_code STRING
, treatment_type_desc STRING
, treatment_group_id INT64
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
