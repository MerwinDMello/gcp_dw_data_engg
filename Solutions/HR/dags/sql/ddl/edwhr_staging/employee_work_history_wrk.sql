CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_work_history_wrk
(
  employee_work_history_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  employee_talent_profile_sid INT64,
  employee_sid INT64,
  employee_num INT64,
  previous_work_address_sid INT64,
  work_history_company_name STRING,
  work_history_job_title_text STRING,
  work_history_desc STRING,
  work_history_start_date DATE,
  work_history_end_date DATE,
  lawson_company_num INT64,
  process_level_code STRING,
  valid_to_date DATETIME,
  source_system_key STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);
