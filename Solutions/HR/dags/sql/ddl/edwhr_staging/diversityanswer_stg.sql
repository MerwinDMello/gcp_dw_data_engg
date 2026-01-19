CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.diversityanswer_stg
(
  file_date DATE,
  diversityanswer_number NUMERIC(29) NOT NULL,
  candidate_number NUMERIC(29),
  creationdate STRING,
  question_number NUMERIC(29),
  possibleanswer_number NUMERIC(29),
  origin_number NUMERIC(29),
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;