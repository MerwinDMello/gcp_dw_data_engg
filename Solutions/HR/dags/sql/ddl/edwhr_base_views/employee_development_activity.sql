CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_development_activity
AS SELECT
    employee_development_activity.employee_development_activity_sid,
    employee_development_activity.valid_from_date,
    employee_development_activity.employee_talent_profile_sid,
    employee_development_activity.employee_sid,
    employee_development_activity.development_activity_name,
    employee_development_activity.development_activity_desc,
    employee_development_activity.catalog_activity_name,
    employee_development_activity.catalog_activity_desc,
    employee_development_activity.development_activity_status_id,
    employee_development_activity.development_activity_priority_id,
    employee_development_activity.development_activity_start_date,
    employee_development_activity.development_activity_end_date,
    employee_development_activity.development_activity_hour_text,
    employee_development_activity.development_activity_comment_text,
    employee_development_activity.employee_num,
    employee_development_activity.lawson_company_num,
    employee_development_activity.process_level_code,
    employee_development_activity.valid_to_date,
    employee_development_activity.source_system_key,
    employee_development_activity.source_system_code,
    employee_development_activity.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_development_activity;