create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_wrk4`
(
  sk NUMERIC,
  sk_source_txt STRING,
  sk_type STRING,
  dw_last_update_date_time DATETIME NOT NULL
)
