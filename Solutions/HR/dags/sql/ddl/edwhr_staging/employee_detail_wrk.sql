create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_detail_wrk`
(
  employee_detail_code STRING NOT NULL,
  employee_sid INT64 NOT NULL,
  applicant_type_id INT64 NOT NULL,
  eff_from_date DATE NOT NULL,
  eff_to_date DATE NOT NULL,
  detail_value_alphanumeric_text STRING,
  detail_value_num INT64,
  detail_value_date DATE,
  employee_num INT64,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
