create table if not exists `{{ params.param_hr_core_dataset_name }}.bi_employee_detail`
(
  employee_sid INT64 NOT NULL,
  employee_num INT64,
  employee_first_name STRING,
  employee_last_name STRING,
  employee_middle_name STRING,
  ethnic_origin_code STRING,
  gender_code STRING,
  adjusted_hire_date DATE,
  birth_date DATE,
  acute_experience_start_date DATE,
  lawson_company_num INT64 NOT NULL,
  process_level_code STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY employee_sid;