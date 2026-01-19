  CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_wrk
  (
    requisition_number STRING,
    process_level_code STRING,
    data_type STRING,
    dw_last_update_date_time DATETIME
  );