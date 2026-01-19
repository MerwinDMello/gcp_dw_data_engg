/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_development_activity AS SELECT
    a.employee_development_activity_sid,
    a.valid_from_date,
    a.employee_talent_profile_sid,
    a.employee_sid,
    a.development_activity_name,
    a.development_activity_desc,
    a.catalog_activity_name,
    a.catalog_activity_desc,
    a.development_activity_status_id,
    a.development_activity_priority_id,
    a.development_activity_start_date,
    a.development_activity_end_date,
    a.development_activity_hour_text,
    a.development_activity_comment_text,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.valid_to_date,
    a.source_system_key,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.employee_development_activity AS a
;
