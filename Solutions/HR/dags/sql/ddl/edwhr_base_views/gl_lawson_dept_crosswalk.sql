create or replace view `{{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk`
AS SELECT
    gl_lawson_dept_crosswalk.gl_company_num,
    gl_lawson_dept_crosswalk.account_unit_num,
    gl_lawson_dept_crosswalk.process_level_code,
    gl_lawson_dept_crosswalk.valid_from_date,
    gl_lawson_dept_crosswalk.valid_to_date,
    gl_lawson_dept_crosswalk.coid,
    gl_lawson_dept_crosswalk.unit_num,
    gl_lawson_dept_crosswalk.dept_num,
    gl_lawson_dept_crosswalk.lawson_company_num,
    gl_lawson_dept_crosswalk.security_key_text,
    gl_lawson_dept_crosswalk.company_code,
    gl_lawson_dept_crosswalk.source_system_code,
    gl_lawson_dept_crosswalk.dw_last_update_date_time
  FROM
   {{ params.param_hr_core_dataset_name }}.gl_lawson_dept_crosswalk;