/*edwhr_staging.glint_question */
CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.glint_question
(
  survey_creation_date DATETIME,
  survey_cycle_uuid STRING,
  survey_cycle_title STRING,
  question_order INT64,
  question_label STRING,
  question_text STRING,
  question_type STRING,
  dw_last_update_date_time DATETIME
)
;