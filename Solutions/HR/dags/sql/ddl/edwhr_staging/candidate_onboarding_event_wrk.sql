CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_wrk
(
  candidate_onboarding_event_sid INT64,
  valid_from_date DATETIME,
  event_type_id STRING,
  recruitment_requisition_num_text STRING,
  completed_date DATETIME,
  candidate_sid INT64,
  resource_screening_package_num INT64,
  sequence_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME NOT NULL
);