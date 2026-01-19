CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.taleo_questiontype
(
  file_date DATE,
  qt_num STRING,
  description STRING,
  dw_last_update_date_time DATETIME
);