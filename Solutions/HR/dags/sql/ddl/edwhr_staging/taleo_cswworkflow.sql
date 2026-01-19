CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_cswworkflow
(
  number INT64,
  available INT64,
  description STRING,
  isdefault STRING,
  mnemonic STRING,
  name STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);