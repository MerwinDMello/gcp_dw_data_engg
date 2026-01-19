create table if not exists `{{ params.param_hr_stage_dataset_name }}.job_class_wrk`
(
  job_class_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  hr_company_sid INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  job_class_code STRING NOT NULL,
  job_class_desc STRING NOT NULL,
  eff_to_date DATE NOT NULL,
  active_dw_ind STRING NOT NULL,
  process_level_code STRING NOT NULL,
  security_key_text STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
