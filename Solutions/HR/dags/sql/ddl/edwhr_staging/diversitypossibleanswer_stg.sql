CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.diversitypossibleanswer_stg
(
  file_date DATE,
  diversitypossibleanswer_number NUMERIC(29) NOT NULL,
  question_number NUMERIC(29),
  description STRING,
  sequence NUMERIC(29),
  possibleanswertype_number NUMERIC(29),
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;