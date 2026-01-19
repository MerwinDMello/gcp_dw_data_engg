CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_question
(
  file_date DATE,
  question_num STRING,
  code STRING,
  creationdate STRING,
  description STRING,
  explanationintro STRING,
  lastmodifieddate STRING,
  question_name STRING,
  eeousaversion STRING,
  eequestiontype_number STRING,
  status_number STRING,
  typeofquestion_number STRING,
  dw_last_update_date_time DATETIME
);