create table if not exists `{{ params.param_hr_stage_dataset_name }}.junc_employee_status_wrk`
(
  employee_sid INT64 NOT NULL,
  status_sid INT64 NOT NULL,
  status_type_code STRING NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  employee_num INT64 NOT NULL,
  status_code STRING NOT NULL,
  delete_ind STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL,
  lawson_company_num INT64,
  process_level_code STRING
)
