/*edwhr_staging.ref_glint_survey_wrk*/
CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.ref_glint_survey_wrk
(
  eff_from_date DATE,
  survey_category_num INT64,
  survey_category_code STRING,
  survey_category_text STRING,
  eff_to_date DATE,
  survey_group_text STRING,
  survey_date DATE,
  survey_start_date DATE,
  survey_end_date DATE,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;