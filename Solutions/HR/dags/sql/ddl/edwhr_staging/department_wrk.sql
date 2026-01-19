create table if not exists `{{ params.param_hr_stage_dataset_name }}.department_wrk`
(
  dept_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  process_level_sid INT64 NOT NULL,
  dept_code STRING NOT NULL,
  dept_name STRING NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  active_dw_ind STRING NOT NULL,
  eff_to_date DATE NOT NULL,
  security_key_text STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
