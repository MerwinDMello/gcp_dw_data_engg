CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.personnel_time_wrk`
(
  employee_num INT64 NOT NULL,
  process_level_code STRING,
  clock_library_code STRING,
  personnel_name STRING,
  hire_date_time DATETIME,
  lawson_company_num INT64,
  job_code STRING,
  dept_code STRING,
  pay_type_code STRING,
  termination_date DATE,
  employee_34_login_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME NOT NULL
);