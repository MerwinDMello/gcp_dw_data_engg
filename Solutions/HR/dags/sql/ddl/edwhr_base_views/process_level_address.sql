create or replace view `{{ params.param_hr_base_views_dataset_name }}.process_level_address`
AS SELECT
    process_level_address.process_level_sid,
    process_level_address.valid_from_date,
    process_level_address.addr_sid,
    process_level_address.valid_to_date,
    process_level_address.lawson_company_num,
    process_level_address.process_level_code,
    process_level_address.security_key_text,
    process_level_address.source_system_code,
    process_level_address.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.process_level_address;