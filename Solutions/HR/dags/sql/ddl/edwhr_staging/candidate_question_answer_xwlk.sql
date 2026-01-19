CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.candidate_question_answer_xwlk
(
  candidate INT64,
  jobreqquestion INT64,
  jobrequisition INT64,
  jobreqquestionanswer STRING,
  dw_last_update_date_time DATETIME
);