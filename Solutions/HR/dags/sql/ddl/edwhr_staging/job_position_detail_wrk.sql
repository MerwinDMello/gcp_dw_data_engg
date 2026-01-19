create table if not exists `{{ params.param_hr_stage_dataset_name }}.job_position_detail_wrk`
(
  position_sid INT64 NOT NULL,
  position_type_id STRING NOT NULL,
  position_detail_code STRING NOT NULL,
  eff_from_date DATE NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  eff_to_date DATE NOT NULL,
  detail_value_alphanumeric_text STRING,
  detail_value_num INT64,
  detail_value_date DATE,
  lawson_object_id INT64,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
