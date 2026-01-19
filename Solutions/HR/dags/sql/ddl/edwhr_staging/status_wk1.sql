create table if not exists `{{ params.param_hr_stage_dataset_name }}.status_wk1`
(
  company INT64,
  a_field STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
