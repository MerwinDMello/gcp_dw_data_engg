CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.communication_device
AS SELECT
    communication_device.communication_device_sid,
    communication_device.communication_device_value,
    communication_device.source_system_code,
    communication_device.dw_last_update_date_time
  FROM
  {{ params.param_hr_core_dataset_name }}.communication_device;
