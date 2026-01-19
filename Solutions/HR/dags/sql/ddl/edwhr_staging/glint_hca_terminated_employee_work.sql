/*edwhr_staging.glint_hca_terminated_employee_work*/
CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.glint_hca_terminated_employee_work
(
  status STRING,
  employee_status STRING,
  employee_status_desc STRING,
  group_num STRING,
  division_num STRING,
  market_num STRING,
  hr_company_curr INT64,
  coid STRING,
  process_level_home_curr STRING,
  dept_num_home_curr STRING,
  lob STRING,
  sub_lob STRING,
  first_name STRING,
  last_name STRING,
  employee_num INT64,
  employee_3_4_id STRING,
  email_address STRING,
  anniversary_date DATE,
  rn_experience DATE,
  birthdate DATE,
  date_hired DATE,
  action_code STRING,
  action_reason STRING,
  action_eff_date DATE,
  personal_email STRING
);