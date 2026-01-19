/***************************************************************************************
   C U S T O M   S E C U R I T Y   V I E W
****************************************************************************************/
CREATE OR REPLACE VIEW {{params.param_hr_rstc_views_dataset_name}}.employee_work_history AS SELECT
    a.employee_work_history_sid,
    a.valid_from_date,
    a.employee_talent_profile_sid,
    a.employee_sid,
    a.previous_work_address_sid,
    a.work_history_company_name,
    a.work_history_job_title_text,
    a.work_history_desc,
    a.work_history_start_date,
    a.work_history_end_date,
    a.employee_num,
    a.lawson_company_num,
    a.process_level_code,
    a.valid_to_date,
    a.source_system_key,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{params.param_hr_base_views_dataset_name}}.employee_work_history AS a
;
