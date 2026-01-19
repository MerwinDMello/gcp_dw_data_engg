CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_job_board_type_stg
(
  file_date DATE,
  job_board_type_number STRING,
  description STRING,
  dw_last_update_date_time DATETIME
);