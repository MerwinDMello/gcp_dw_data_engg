create table if not exists `{{ params.param_hr_stage_dataset_name }}.process_level_wrk`
(
  process_level_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  hr_company_sid INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  process_level_name STRING NOT NULL,
  process_level_active_code STRING,
  active_dw_ind STRING,
  eff_to_date DATE NOT NULL,
  security_key_text STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL,
  row_id INT64 NOT NULL
)
