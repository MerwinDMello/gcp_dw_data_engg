create table if not exists `{{ params.param_hr_stage_dataset_name }}.applicant_wrk`
(
  applicant_sid INT64,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  applicant_num INT64,
  lawson_company_num INT64,
  process_level_code STRING,
  employee_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
