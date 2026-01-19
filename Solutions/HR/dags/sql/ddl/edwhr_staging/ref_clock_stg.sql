CREATE TABLE IF NOT EXISTS `{{ params.param_hr_stage_dataset_name }}.ref_clock_stg`
(
  kronos_clock_library STRING NOT NULL,
  clock_code STRING NOT NULL,
  clock_desc STRING,
  source_system_code STRING,
  dw_last_update_date_time DATETIME
);