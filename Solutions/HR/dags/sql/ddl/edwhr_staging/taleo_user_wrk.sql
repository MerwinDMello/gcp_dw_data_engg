CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_user_wrk
(
  file_date DATE,
  recruitment_user_sid INT64,
  valid_from_date DATETIME,
  recruitment_user_num INT64,
  valid_to_date DATETIME,
  employee_num INT64,
  employee_34_login_code STRING,
  first_name STRING,
  last_name STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);