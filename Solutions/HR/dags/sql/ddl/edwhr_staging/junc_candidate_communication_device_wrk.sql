CREATE TABLE IF NOT EXISTS {{ params.param_hr_stage_dataset_name }}.junc_candidate_communication_device_wrk
(
 communication_device_sid INT64 NOT NULL,
  candidate_sid INT64,
  communication_device_type_code STRING,
  valid_from_date DATETIME NOT NULL,
  valid_to_date DATETIME,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
);