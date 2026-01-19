CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.offer_status_wrk
(
  file_date DATE,
  offer_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  offer_status_id INT64,
  valid_to_date DATETIME,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);