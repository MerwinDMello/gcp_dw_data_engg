CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_jobschedule
(
  number INT64,
  active INT64,
  code STRING,
  description STRING,
  sequence INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);