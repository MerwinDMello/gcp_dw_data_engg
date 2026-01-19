CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_recruitmentsource
(
  number INT64,
  description STRING,
  networklocation_number INT64,
  objectorigin_number INT64,
  parentsource_number INT64,
  state_number INT64,
  type_number INT64,
  visibility_number INT64,
  partnerid STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);