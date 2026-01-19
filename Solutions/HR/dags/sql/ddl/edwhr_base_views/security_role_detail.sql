create or replace view `{{ params.param_hr_base_views_dataset_name }}.security_role_detail`
AS SELECT
security_role_detail.lawson_company_num,
security_role_detail.process_level_code,
security_role_detail.access_role_code,
security_role_detail.span_code,
security_role_detail.dept_code,
security_role_detail.view_only_span_code,
security_role_detail.last_update_date,
security_role_detail.source_system_code,
security_role_detail.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.security_role_detail;