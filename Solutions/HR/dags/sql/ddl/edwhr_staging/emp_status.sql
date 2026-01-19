create table if not exists `{{ params.param_hr_stage_dataset_name }}.emp_status`
(
  employee_sid INT64 NOT NULL,
  status_from_date DATE,
  status_to_date DATE,
  emp_status_code STRING,
  emp_status_sid INT64,
  lawson_company_num INT64,
  rr1 INT64,
  rr2 INT64,
  dw_last_update_date_time DATETIME NOT NULL
)
