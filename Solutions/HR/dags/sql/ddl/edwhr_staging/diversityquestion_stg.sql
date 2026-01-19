CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.diversityquestion_stg
(
  file_date DATE,
  diversityquestion_number NUMERIC(29) NOT NULL,
  code STRING,
  type_name STRING,
  questiontype_number NUMERIC(29),
  regulationquestiontype_number NUMERIC(29),
  status_number NUMERIC(29),
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;