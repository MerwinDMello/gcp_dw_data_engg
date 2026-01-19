CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.employee_development_activity_wrk
(
  employee_development_activity_sid INT64,
  employee_talent_profile_sid INT64,
  employee_sid INT64,
  employee_num INT64,
  development_activity_name STRING,
  development_activity_desc STRING,
  catalog_activity_name STRING,
  catalog_activity_desc STRING,
  development_activity_status_id INT64,
  development_activity_priority_id INT64,
  development_activity_start_date DATE,
  development_activity_end_date DATE,
  development_activity_hour_text STRING,
  development_activity_comment_text STRING,
  lawson_company_num INT64,
  process_level_code STRING,
  source_system_key STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);
