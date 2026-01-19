create table if not exists `{{ params.param_hr_stage_dataset_name }}.job_code_wrk`
(
  job_code_sid INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  valid_from_date DATETIME NOT NULL,
  job_class_sid INT64 NOT NULL,
  hr_company_sid INT64 NOT NULL,
  lawson_company_num INT64 NOT NULL,
  job_code STRING NOT NULL,
  job_code_desc STRING NOT NULL,
  active_dw_ind STRING NOT NULL,
  process_level_code STRING NOT NULL,
  eeo_category_code STRING,
  eeo_code INT64,
  security_key_text STRING NOT NULL,
  valid_to_date DATETIME NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
