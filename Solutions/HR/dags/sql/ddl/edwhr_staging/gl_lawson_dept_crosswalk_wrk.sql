create table if not exists `{{ params.param_hr_stage_dataset_name }}.gl_lawson_dept_crosswalk_wrk`
(
  gl_company_num INT64 NOT NULL,
  account_unit_num STRING NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  coid STRING NOT NULL,
  unit_num STRING NOT NULL,
  dept_num STRING NOT NULL,
  process_level_code STRING NOT NULL,
  lawson_company_num INT64 NOT NULL,
  security_key_text STRING NOT NULL,
  company_code STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
