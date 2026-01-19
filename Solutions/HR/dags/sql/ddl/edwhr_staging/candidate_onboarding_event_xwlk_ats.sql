  CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_onboarding_event_xwlk_ats
  (
    jobrequisition STRING,
    candidate STRING,
    event_type_id STRING,
    dw_last_update_date_time DATETIME
  );