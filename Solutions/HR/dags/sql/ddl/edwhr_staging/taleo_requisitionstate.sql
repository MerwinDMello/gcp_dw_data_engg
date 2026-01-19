CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_requisitionstate
(
  file_date DATE,
  requisitionstate_number INT64,
  description STRING,
  parentstate_number INT64,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);