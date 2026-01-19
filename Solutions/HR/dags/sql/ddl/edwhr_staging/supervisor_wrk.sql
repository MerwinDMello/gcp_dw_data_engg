create table if not exists `{{ params.param_hr_stage_dataset_name }}.supervisor_wrk`
(
  supervisor_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  valid_from_date DATETIME NOT NULL,
  employee_sid INT64,
  employee_num INT64,
  hr_company_sid INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  reporting_supervisor_sid INT64,
  supervisor_code STRING NOT NULL,
  supervisor_desc STRING,
  officer_code STRING,
  process_level_code STRING NOT NULL,
  eff_to_date DATE NOT NULL,
  active_dw_ind STRING NOT NULL,
  security_key_text STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
