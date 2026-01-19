create table if not exists `{{ params.param_hr_stage_dataset_name }}.jobrelate`
(
  type STRING,
  pers_code STRING,
  required_flag STRING,
  pct_of_time NUMERIC,
  profic_level STRING,
  wght NUMERIC,
  subject_code STRING,
  job_code STRING,
  position STRING,
  company INT64,
  process_level STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
