create table if not exists `{{ params.param_hr_stage_dataset_name }}.hr_company_wrk`
(
  hr_company_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  company_code STRING NOT NULL,
  lawson_company_num INT64 NOT NULL,
  company_name STRING NOT NULL,
  valid_to_date DATETIME NOT NULL,
  active_dw_ind STRING NOT NULL,
  security_key_text STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
