CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.survey_response_master_wrk
(
  eff_from_date DATE NOT NULL,
  survey_question_sid INT64 NOT NULL,
  response_value_text STRING NOT NULL,
  response_label_text STRING,
  eff_to_date DATE NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);