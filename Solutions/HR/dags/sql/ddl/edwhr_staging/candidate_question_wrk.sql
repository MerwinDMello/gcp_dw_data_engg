CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_question_wrk
(
  question_sid INT64 NOT NULL,
  question_num INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  creation_date DATE,
  question_desc STRING,
  question_code STRING,
  last_modified_date DATE,
  requisition_num INT64,
  question_type_num INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);