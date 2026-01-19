CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.hca_pat_resp_wrk1
(
  hca_unique STRING,
  surv_id NUMERIC(29),
  adj_samp STRING,
  survey_type STRING,
  pg_unit STRING,
  disdate DATE,
  recdate DATE,
  mde STRING,
  question_id STRING,
  resp STRING,
  coid STRING,
  dw_last_update_date_time DATETIME
);