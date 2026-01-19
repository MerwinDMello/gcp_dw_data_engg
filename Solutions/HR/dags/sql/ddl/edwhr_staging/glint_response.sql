CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.glint_response
(
  survey_creation_date STRING,
  survey_completion_date STRING,
  survey_sent_date STRING,
  survey_cycle_uuid STRING,
  survey_cycle_title STRING,
  employee_num STRING,
  first_name STRING,
  last_name STRING,
  email STRING,
  --employee_id STRING,
  question STRING,
  response STRING,
  comments STRING,
  dw_last_update_date_time DATETIME
)
;