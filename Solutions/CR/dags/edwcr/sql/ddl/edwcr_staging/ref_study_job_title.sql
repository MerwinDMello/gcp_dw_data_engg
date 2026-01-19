CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.ref_study_job_title (
job_title_id INT64 NOT NULL
, job_title_source_code INT64 NOT NULL
, job_title_name STRING NOT NULL
, job_title_desc STRING
, update_ind STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
