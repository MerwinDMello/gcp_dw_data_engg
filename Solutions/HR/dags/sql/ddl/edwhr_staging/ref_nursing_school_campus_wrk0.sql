CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk0
(
  campus_name STRING NOT NULL,
  campus_code STRING,
  addr_sid INT64,
  dw_last_update_date_time DATETIME NOT NULL
);