CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_questionanswer
(
  file_date DATE,
  qns_num STRING,
  candidate_number STRING,
  creationdate STRING,
  question_number STRING,
  answer_number STRING,
  explanation STRING,
  disqualifierresult_number STRING,
  infofeeder_number STRING,
  dw_last_update_date_time DATETIME
);