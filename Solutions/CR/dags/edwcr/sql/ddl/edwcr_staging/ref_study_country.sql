CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_study_country (
country_id INT64 NOT NULL
, country_code2 STRING
, country_code3 STRING
, country_name STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
