CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_recruitment_location_wrk
(
  file_date DATE,
  location_num NUMERIC NOT NULL,
  level_num INT64,
  location_name STRING,
  location_code_text STRING,
  work_location_code_text STRING,
  addr_sid INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);