CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.ref_job_board_type
(
  job_board_type_id INT64 NOT NULL,
  job_board_type_desc STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY job_board_type_id;