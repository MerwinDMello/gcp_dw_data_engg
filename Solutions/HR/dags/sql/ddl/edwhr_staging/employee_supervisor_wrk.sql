create table if not exists `{{ params.param_hr_stage_dataset_name }}.employee_supervisor_wrk`
(
  employee_sid INT64 NOT NULL,
  supervisor_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME NOT NULL,
  employee_num INT64 NOT NULL,
  supervisor_code STRING NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL,
  lawson_company_num INT64,
  process_level_code STRING
)
