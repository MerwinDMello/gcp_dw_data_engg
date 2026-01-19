CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk
(
  candidate_profile_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  candidate_sid INT64,
  profile_medium_id INT64,
  candidate_profile_num NUMERIC(29),
  submission_date DATE,
  submission_date_time DATETIME,
  completion_date DATE,
  completion_date_time DATETIME,
  creation_date DATE,
  creation_date_time DATETIME,
  recruitment_source_id INT64,
  recruitment_source_auto_filled_sw INT64,
  job_application_num INT64,
  requisition_num INT64,
  candidate_num INT64,
  valid_to_date DATETIME NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);