CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_history_wrk
(
  file_date DATE,
  recruitment_requisition_sid INT64 NOT NULL,
  creation_date_time DATETIME NOT NULL,
  requisition_status_id INT64 NOT NULL,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  closed_date_time DATETIME,
  requisition_creator_user_sid INT64,
  recruitier_owner_user_sid INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);
