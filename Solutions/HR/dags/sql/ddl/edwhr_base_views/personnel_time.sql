CREATE OR REPLACE VIEW `{{ params.param_hr_base_views_dataset_name }}.personnel_time`
AS select
    personnel_time.employee_num,
    personnel_time.process_level_code,
    personnel_time.clock_library_code,
    personnel_time.valid_from_date,
    personnel_time.valid_to_date,
    personnel_time.personnel_name,
    personnel_time.hire_date_time,
    personnel_time.lawson_company_num,
    personnel_time.job_code,
    personnel_time.dept_code,
    personnel_time.pay_type_code,
    personnel_time.termination_date,
    personnel_time.employee_34_login_code,
    personnel_time.source_system_code,
    personnel_time.dw_last_update_date_time  
    FROM
    {{ params.param_hr_core_dataset_name }}.personnel_time;