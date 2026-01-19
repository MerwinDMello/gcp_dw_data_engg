CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hca_quest_mstr
(
  qtype STRING,
  ptype INT64,
  category STRING,
  sub_category STRING,
  qshort_txt STRING,
  qtxt STRING,
  question_id STRING,
  survey_ord STRING,
  tboxvalue INT64,
  tbox_high_val INT64,
  std_flag STRING,
  ignore_val STRING,
  dw_last_update_date_time DATETIME
);