CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_cswstep
(
  number INT64,
  available INT64,
  description STRING,
  mnemonic STRING,
  name STRING,
  shortname STRING,
  applicationstate_number INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;