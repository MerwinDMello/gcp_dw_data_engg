create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_position_security_role`
AS SELECT
    employee_position_security_role.employee_sid,
    employee_position_security_role.position_sid,
    employee_position_security_role.eff_from_date,
    employee_position_security_role.span_code,
    employee_position_security_role.access_role_code,
    employee_position_security_role.job_code,
    employee_position_security_role.employee_num,
    employee_position_security_role.coid,
    employee_position_security_role.company_code,
    employee_position_security_role.dept_code,
    employee_position_security_role.position_code,
    employee_position_security_role.employee_34_login_code,
    employee_position_security_role.lawson_company_num,
    employee_position_security_role.process_level_code,
    employee_position_security_role.source_system_code,
    employee_position_security_role.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_position_security_role;