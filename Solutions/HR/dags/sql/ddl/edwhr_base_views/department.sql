create or replace view `{{ params.param_hr_base_views_dataset_name }}.department`
AS SELECT
    department.dept_sid,
    department.valid_from_date,
    department.valid_to_date,
    department.process_level_sid,
    department.dept_code,
    department.dept_name,
    department.lawson_company_num,
    department.process_level_code,
    department.source_system_code,
    department.active_dw_ind,
    department.security_key_text,
    department.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.department;