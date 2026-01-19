CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_resource_wrk
(
  resource_screening_package_num INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  candidate_sid INT64,
  recruitment_requisition_sid INT64,
  valid_to_date DATETIME NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);