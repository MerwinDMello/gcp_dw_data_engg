create or replace view `{{ params.param_hr_base_views_dataset_name }}.process_level`
AS SELECT
    process_level.process_level_sid,
    process_level.valid_from_date,
    process_level.valid_to_date,
    process_level.hr_company_sid,
    process_level.lawson_company_num,
    process_level.process_level_code,
    process_level.process_level_name,
    process_level.process_level_active_code,
    process_level.active_dw_ind,
    process_level.security_key_text,
    process_level.source_system_code,
    process_level.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.process_level;