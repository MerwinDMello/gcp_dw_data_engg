CREATE TABLE IF NOT EXISTS {{ params.param_hr_core_dataset_name }}.communication_device
(
  communication_device_sid INT64 NOT NULL,
  communication_device_value STRING,
  source_system_code STRING NOT NULL,
  dw_last_update_date_time DATETIME NOT NULL
)
CLUSTER BY communication_device_sid;
