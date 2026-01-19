CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.krono_combined_edw_employee_reject`
(
  employee_num STRING NOT NULL,
  process_level_code STRING,
  clock_library_code STRING,
  personnel_name STRING,
  hire_date DATE,
  hr_company STRING,
  job_code STRING,
  dept_code STRING,
  pay_type_code STRING,
  termination_date DATE,
  employee_34_login_code STRING,
  dw_last_update_date_time DATETIME NOT NULL
);