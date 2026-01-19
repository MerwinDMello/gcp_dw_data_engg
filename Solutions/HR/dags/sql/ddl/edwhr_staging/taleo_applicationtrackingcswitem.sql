CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_applicationtrackingcswitem
(
  file_date DATE,
  number INT64,
  commenttext STRING,
  creationdate DATETIME,
  detail STRING,
  eventdate DATETIME,
  isconditional STRING,
  profilelocale_iso STRING,
  event_number INT64,
  message_number STRING,
  profileinformation_number INT64,
  status_number STRING,
  step_number INT64,
  substitute_userno STRING,
  user_userno INT64,
  workflow_number INT64,
  reverted STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);