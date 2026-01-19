CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_job_board_wrk
(
  file_date DATE,
  recruitment_job_sid INT64,
  job_board_id INT64,
  posting_board_type_id INT64,
  posting_status_id INT64,
  posting_date DATE,
  unposting_date DATE,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);