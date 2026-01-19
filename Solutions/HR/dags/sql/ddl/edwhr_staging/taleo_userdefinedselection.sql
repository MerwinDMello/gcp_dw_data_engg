CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_userdefinedselection
(
  number INT64,
  code STRING,
  effectivedatingenabled STRING,
  large STRING,
  name STRING,
  origin_number INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);