CREATE OR REPLACE VIEW {{ params.param_hr_base_views_dataset_name }}.employee_work_history
AS SELECT
    employee_work_history.employee_work_history_sid,
    employee_work_history.valid_from_date,
    employee_work_history.employee_talent_profile_sid,
    employee_work_history.employee_sid,
    employee_work_history.previous_work_address_sid,
    employee_work_history.work_history_company_name,
    employee_work_history.work_history_job_title_text,
    employee_work_history.work_history_desc,
    employee_work_history.work_history_start_date,
    employee_work_history.work_history_end_date,
    employee_work_history.employee_num,
    employee_work_history.lawson_company_num,
    employee_work_history.process_level_code,
    employee_work_history.valid_to_date,
    employee_work_history.source_system_key,
    employee_work_history.source_system_code,
    employee_work_history.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_work_history;