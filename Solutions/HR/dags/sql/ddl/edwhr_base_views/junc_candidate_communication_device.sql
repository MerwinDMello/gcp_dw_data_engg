CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.junc_candidate_communication_device
AS SELECT
    junc_candidate_communication_device.communication_device_sid,
    junc_candidate_communication_device.candidate_sid,
    junc_candidate_communication_device.communication_device_type_code,
    junc_candidate_communication_device.valid_from_date,
    junc_candidate_communication_device.valid_to_date,
    junc_candidate_communication_device.source_system_code,
    junc_candidate_communication_device.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.junc_candidate_communication_device;