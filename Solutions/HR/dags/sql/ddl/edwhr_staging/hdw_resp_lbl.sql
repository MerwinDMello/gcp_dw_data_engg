CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hdw_resp_lbl
(
  question_id STRING,
  response INT64,
  response_label STRING,
  dw_last_update_date_time DATETIME
);