create table if not exists `{{ params.param_hr_stage_dataset_name }}.hr_employee_history_wrk`
(
  employee_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  lawson_element_num INT64 NOT NULL,
  hr_employee_element_date DATE NOT NULL,
  lawson_object_id INT64 NOT NULL,
  sequence_num INT64 NOT NULL,
  hr_employee_value_num NUMERIC,
  hr_employee_value_alphanumeric_text STRING,
  hr_employee_value_date DATE,
  action_object_identifier INT64,
  user_3_4_login_code STRING,
  data_type_flag STRING,
  position_level_sequence_num INT64,
  last_update_date DATE,
  last_update_time TIME,
  valid_to_date DATETIME,
  lawson_company_num INT64 NOT NULL,
  employee_num INT64,
  process_level_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
