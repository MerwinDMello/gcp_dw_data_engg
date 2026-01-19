CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_cswstatus
(
  number STRING,
  available STRING,
  description STRING,
  mnemonic STRING,
  name STRING,
  applicationstate_number STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);