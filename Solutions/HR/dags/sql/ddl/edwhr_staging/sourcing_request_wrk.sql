CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk
(
  sourcing_request_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  recruitment_requisition_sid INT64,
  job_board_id INT64 NOT NULL,
  source_request_status_id INT64,
  job_board_type_id INT64,
  valid_to_date DATETIME,
  posting_date DATE,
  unposting_date DATE,
  creation_date DATE,
  requisition_num INT64 NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);