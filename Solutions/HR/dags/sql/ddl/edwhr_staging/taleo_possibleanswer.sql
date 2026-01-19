CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_possibleanswer
(
  file_date DATE,
  ans_num STRING,
  question_number STRING,
  description STRING,
  displaysequence STRING,
  notspecified STRING,
  disqualifierresult_number STRING,
  dw_last_update_date_time DATETIME
);