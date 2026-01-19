create or replace view `{{ params.param_hr_base_views_dataset_name }}.status`
AS SELECT
    status.status_sid,
    status.valid_from_date,
    status.valid_to_date,
    status.hr_company_sid,
    status.lawson_company_num,
    status.status_type_code,
    status.status_code,
    status.status_desc,
    status.active_dw_ind,
    status.process_level_code,
    status.security_key_text,
    status.source_system_code,
    status.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.status;