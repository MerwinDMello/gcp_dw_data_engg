create or replace view `{{ params.param_hr_base_views_dataset_name }}.employee_security_role`
AS SELECT
employee_security_role.employee_34_login_code,
employee_security_role.system_code,
employee_security_role.security_role_code,
employee_security_role.access_role_code,
employee_security_role.span_code,
employee_security_role.create_date,
employee_security_role.creator_user_id_code,
employee_security_role.last_update_date,
employee_security_role.active_ind,
employee_security_role.source_system_code,
employee_security_role.dw_last_update_date_time
  FROM
    {{ params.param_hr_core_dataset_name }}.employee_security_role;