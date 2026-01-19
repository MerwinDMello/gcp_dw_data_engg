CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_udfdefinition
(
  description STRING,
  entity STRING,
  id STRING,
  cardinality STRING,
  type_number INT64,
  userdefinedselection_number INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);