CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_diagnosis_detail_stg (
diagnosis_detail_desc STRING
, diagnosis_indicator_text STRING
, source_system_code STRING
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
