create or replace view `{{ params.param_hr_base_views_dataset_name }}.job_position_detail`
AS SELECT
    job_position_detail.position_sid,
    job_position_detail.position_type_id,
    job_position_detail.position_detail_code,
    job_position_detail.eff_from_date,
    job_position_detail.valid_from_date,
    job_position_detail.valid_to_date,
    job_position_detail.eff_to_date,
    job_position_detail.detail_value_alphanumeric_text,
    job_position_detail.detail_value_num,
    job_position_detail.detail_value_date,
    job_position_detail.lawson_object_id,
    job_position_detail.lawson_company_num,
    job_position_detail.process_level_code,
    job_position_detail.source_system_code,
    job_position_detail.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.job_position_detail;