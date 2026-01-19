CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_nurse_detail_wrk (
employee_sid INT64 NOT NULL
, report_date DATE
, ncsbn_id NUMERIC(10,0)
, employee_34_login_code STRING
, employee_num INT64
, lawson_company_num INT64 NOT NULL
, process_level_code STRING NOT NULL
, license_num_text STRING
, source_system_code STRING NOT NULL
, dw_last_update_date_time DATETIME NOT NULL
)
  ;
