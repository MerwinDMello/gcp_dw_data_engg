CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_cswmotive
(
  cswmotive_number INT64,
  available INT64,
  mnemonic STRING,
  name STRING,
  description STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);