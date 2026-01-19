CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.survey_question_wrk
(
  eff_from_date DATE NOT NULL,
  survey_sid INT64 NOT NULL,
  survey_sub_category_text STRING,
  question_id STRING,
  question_type_code STRING NOT NULL,
  question_short_name STRING,
  question_desc STRING,
  question_seq_num INT64 NOT NULL,
  top_box_num INT64,
  top_box_high_num INT64,
  measure_id_text STRING,
  legacy_question_id INT64,
  standard_flag STRING,
  ignore_value INT64,
  eff_to_date DATE NOT NULL,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);