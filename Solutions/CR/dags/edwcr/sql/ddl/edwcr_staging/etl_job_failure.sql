CREATE OR REPLACE TABLE {{ params.param_cr_stage_dataset_name }}.etl_job_failure (
job_name STRING NOT NULL
, job_start_date_time DATETIME NOT NULL
, job_fail_date_time DATETIME NOT NULL
, failure_code INT64
, failure_message STRING
, resolution STRING
, resolution_time STRING
)
  ;
