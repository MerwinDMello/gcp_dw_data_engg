CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_education_profile_wrk
(
  row_number_1 INT64,
  employee_education_profile_sid INT64,
  employee_talent_profile_sid INT64,
  employee_sid INT64,
  employee_num INT64,
  employee_education_type_code STRING,
  detail_value_alpahnumeric_text STRING,
  detail_value_num NUMERIC(29),
  detail_value_date DATE,
  lawson_company_num INT64,
  process_level_code STRING,
  source_system_key STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);
