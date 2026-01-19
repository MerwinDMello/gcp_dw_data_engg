CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_wrk
(
  question_answer_sid INT64 NOT NULL,
  question_answer_num INT64,
  candidate_sid INT64,
  valid_from_date DATETIME,
  creation_date DATE,
  question_sid INT64,
  answer_sid INT64,
  comment_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);