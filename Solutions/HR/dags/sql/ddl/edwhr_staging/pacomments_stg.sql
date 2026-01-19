create table if not exists `{{ params.param_hr_stage_dataset_name }}.pacomments_stg`
(
  company INT64,
  emp_app INT64,
  cmt_type STRING,
  employee INT64,
  job_code STRING,
  action_code STRING,
  ln_nbr INT64,
  seq_nbr INT64,
  cmt_text STRING,
  r_date DATE,
  print_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
