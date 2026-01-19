CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_sourcing_request_stg
(
  file_date DATE,
  requisition_number STRING,
  jobboard_number STRING,
  creationdate STRING,
  applyonlineenabled STRING,
  closedate STRING,
  opendate STRING,
  request_status_number INT64,
  dw_last_update_date_time DATETIME
);