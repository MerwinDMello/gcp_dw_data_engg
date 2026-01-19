CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.submission_tracking_wrk
(
  file_date DATE,
  submission_tracking_sid INT64,
  candidate_profile_sid INT64,
  submission_tracking_num INT64,
  creation_date_time DATETIME,
  event_date_time DATETIME,
  event_detail_text STRING,
  submission_event_id INT64,
  tracking_user_sid INT64,
  tracking_step_id INT64,
  tracking_workflow_id INT64,
  sub_status_desc STRING,
  moved_by_text STRING,
  step_reverted_ind STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);
