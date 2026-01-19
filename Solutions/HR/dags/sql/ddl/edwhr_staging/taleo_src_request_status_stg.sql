CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_src_request_status_stg
(
  file_date DATE,
  src_request_status_number STRING,
  description STRING,
  dw_last_update_date_time DATETIME
);