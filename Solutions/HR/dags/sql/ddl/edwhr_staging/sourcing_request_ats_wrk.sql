CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.sourcing_request_ats_wrk
(
  recruitment_requisition_sid INT64,
  jobrequisition INT64,
  job_board_type_id INT64,
  source_request_status_id INT64,
  posting_date DATE,
  unposting_date DATE,
  creation_date DATE
);