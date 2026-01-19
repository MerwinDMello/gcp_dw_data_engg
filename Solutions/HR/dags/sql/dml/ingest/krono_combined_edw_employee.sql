update {{params.param_hr_stage_dataset_name}}.krono_combined_edw_employee
set	process_level_code = nullif(ltrim(process_level_code),''),			
    clock_library_code = nullif(ltrim(clock_library_code),''),			
    personnel_name = nullif(ltrim(personnel_name),''),			
    hr_company = nullif(ltrim(hr_company),''),			
    job_code = nullif(ltrim(job_code),''),			
    dept_code = nullif(ltrim(dept_code),''),			
    pay_type_code = nullif(ltrim(pay_type_code),''),			
    employee_34_login_code = nullif(ltrim(employee_34_login_code),''),
    dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND)
where dw_last_update_date_time is null;