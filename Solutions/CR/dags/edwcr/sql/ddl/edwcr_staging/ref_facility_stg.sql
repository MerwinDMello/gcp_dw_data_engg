CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_facility_stg (
facility_name STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
