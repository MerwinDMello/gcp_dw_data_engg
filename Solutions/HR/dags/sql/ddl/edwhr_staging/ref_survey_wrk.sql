CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_survey_wrk
(
  eff_from_date DATE,
  survey_category_num INT64,
  survey_category_code STRING,
  survey_category_text STRING,
  eff_to_date DATE,
  survey_group_text STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);