CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_study_type (
study_type_id INT64 NOT NULL
, study_source_code INT64 NOT NULL
, study_type_name STRING NOT NULL
, study_type_desc STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
