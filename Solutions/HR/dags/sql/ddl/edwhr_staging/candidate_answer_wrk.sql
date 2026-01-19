CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_answer_wrk
(
  answer_sid INT64 NOT NULL,
  answer_num INT64 NOT NULL,
  valid_from_date DATETIME,
  valid_to_date DATETIME,
  answer_desc STRING,
  question_sid INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);