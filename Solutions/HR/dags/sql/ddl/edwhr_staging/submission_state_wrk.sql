CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_state_wrk
(
  file_date DATE,
  submission_sid INT64,
  submission_state_id INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);