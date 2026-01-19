CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_wrk
(
  candidate_onboarding_sid INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  requisition_sid INT64,
  employee_sid INT64,
  candidate_sid INT64,
  candidate_first_name STRING,
  candidate_last_name STRING,
  tour_start_date DATE,
  tour_id INT64,
  tour_status_id INT64,
  tour_completion_pct NUMERIC(32, 3),
  workflow_id INT64,
  workflow_status_id INT64,
  email_sent_status_id INT64,
  onboarding_confirmation_date DATE,
  recruitment_requisition_num_text STRING,
  process_level_code STRING,
  applicant_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME NOT NULL
);