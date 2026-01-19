CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_udselement
(
  number INT64,
  userdefinedselection_number INT64,
  active STRING,
  code STRING,
  complete STRING,
  description STRING,
  effectivefrom DATETIME,
  effectiveuntil DATETIME,
  lastmodifieddate DATETIME,
  sequence STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);