CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.diversitypossanstype_stg
(
  file_date DATE,
  diversitypossanstype_number NUMERIC(29) NOT NULL,
  code STRING NOT NULL,
  type_name STRING,
  origin_number NUMERIC(29) NOT NULL,
  status_number NUMERIC(29) NOT NULL,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
)
;