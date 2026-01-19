CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_tracking_status_wrk
(
file_date DATETIME,
  submission_tracking_sid INT64,
  submission_status_id STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL  
);