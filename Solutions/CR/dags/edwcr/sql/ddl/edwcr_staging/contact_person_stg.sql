CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.contact_person_stg (
contact_person_desc STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
