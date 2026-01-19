CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_wrk
(
  file_date DATE,
  submission_sid INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  submission_num NUMERIC,
  last_modified_date DATE,
  new_submission_sw INT64,
  candidate_sid INT64,
  recruitment_requisition_sid INT64,
  candidate_profile_sid INT64,
  current_submission_status_id INT64 NOT NULL,
  current_submission_step_id INT64 NOT NULL,
  current_submission_workflow_id INT64 NOT NULL,
  requisition_num INT64,
  job_application_num INT64,
  candidate_num INT64,
  matched_from_requisition_num INT64,
  matched_candidate_flag STRING,
  submission_source_code STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);