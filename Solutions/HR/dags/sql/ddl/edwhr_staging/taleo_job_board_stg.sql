CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_job_board_stg
(
  file_date DATE,
  job_board_number STRING,
  ispaying STRING,
  jobboardtype_number STRING,
  recruitmentsource_number STRING,
  dw_last_update_date_time DATETIME
);