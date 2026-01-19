create table if not exists `{{ params.param_hr_stage_dataset_name }}.jobclass`
(
  job_class STRING NOT NULL,
  company INT64 NOT NULL,
  description STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
