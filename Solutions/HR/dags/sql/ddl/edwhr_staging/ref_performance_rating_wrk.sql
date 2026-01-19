CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_performance_rating_wrk
(
  performance_rating_id INT64 NOT NULL,
  performance_rating_desc STRING,
  dw_last_update_date_time DATETIME NOT NULL,
  source_system_code STRING NOT NULL
);
