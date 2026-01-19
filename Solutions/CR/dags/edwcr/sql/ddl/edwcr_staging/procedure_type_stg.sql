CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.procedure_type_stg (
procedure_type_desc STRING
, procedure_sub_type_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
