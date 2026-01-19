
CREATE OR REPLACE VIEW {{ params.param_hr_stnd_views_dataset_name }}.gl_lawson_dept_crosswalk AS SELECT
    a.gl_company_num,
    a.account_unit_num,
    a.process_level_code,
    a.valid_from_date,
    a.valid_to_date,
    a.coid,
    a.unit_num,
    a.dept_num,
    a.lawson_company_num,
    a.security_key_text,
    a.company_code,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    {{ params.param_hr_base_views_dataset_name }}.gl_lawson_dept_crosswalk AS a
;
