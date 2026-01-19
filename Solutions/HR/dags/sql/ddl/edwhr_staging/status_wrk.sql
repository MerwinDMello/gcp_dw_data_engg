create table if not exists `{{ params.param_hr_stage_dataset_name }}.status_wrk`
(
  status_sid NUMERIC,
  eff_from_date DATE,
  hr_company_sid NUMERIC,
  lawson_company_num INT64,
  status_type_code STRING,
  status_code STRING,
  status_desc STRING,
  active_dw_ind STRING,
  eff_to_date DATE,
  process_level_code STRING NOT NULL,
  security_key_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
