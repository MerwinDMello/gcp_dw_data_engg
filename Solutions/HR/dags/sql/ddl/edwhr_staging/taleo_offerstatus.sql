CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_offerstatus
(
  number STRING,
  description STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);