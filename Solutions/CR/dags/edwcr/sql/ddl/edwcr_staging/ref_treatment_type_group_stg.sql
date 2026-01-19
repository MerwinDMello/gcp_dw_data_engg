CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_treatment_type_group_stg (
treatment_type_group_code STRING
, treatment_type_group_desc STRING
, nav_treatment_type_group_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME
)
  ;
