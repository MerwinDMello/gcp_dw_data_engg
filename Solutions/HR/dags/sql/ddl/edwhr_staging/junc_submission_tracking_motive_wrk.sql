CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.junc_submission_tracking_motive_wrk
(
  file_date DATE,
  submission_tracking_sid INT64 NOT NULL,
  tracking_motive_id INT64 NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);