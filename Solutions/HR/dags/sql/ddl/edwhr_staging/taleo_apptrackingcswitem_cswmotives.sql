CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_apptrackingcswitem_cswmotives
(
  file_date DATE,
  atcswitem_number INT64,
  cswmotive_number INT64,
  dw_last_update_date_time DATETIME
);