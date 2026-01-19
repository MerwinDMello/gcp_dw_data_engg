CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_tracking_stg
(
  createstamp DATETIME,
  sub_status_desc STRING,
  candidate_profile_sid INT64,
  submission_event_id INT64,
  step_id INT64,
  workflow_id INT64,
  moved_by_text STRING
);