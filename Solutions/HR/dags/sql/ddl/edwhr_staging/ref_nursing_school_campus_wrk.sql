CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk
(
  campus_id INT64 NOT NULL,
  campus_name STRING NOT NULL,
  campus_code STRING,
  nursing_school_id INT64 NOT NULL,
  addr_sid INT64,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);