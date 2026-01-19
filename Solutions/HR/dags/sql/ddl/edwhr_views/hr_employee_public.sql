
  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_employee_public AS SELECT
      ep.employee_first_name,
      ep.employee_last_name,
      ep.email_text,
      e.employee_34_login_code,
      e.lawson_company_num,
      c.company_name,
      e.process_level_code,
      pl.process_level_name,
      e.dept_code,
      d.dept_name,
      e.primary_facility_ind
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_person AS ep
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.employee AS e ON ep.employee_sid = e.employee_sid
       AND date(e.valid_to_date) = '9999-12-31'
       AND date(ep.valid_to_date) = '9999-12-31'
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS d ON e.dept_sid = d.dept_sid
       AND date(d.valid_to_date) = '9999-12-31'
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.process_level AS pl ON e.process_level_sid = pl.process_level_sid
       AND date(pl.valid_to_date) = '9999-12-31'
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_company AS c ON pl.hr_company_sid = c.hr_company_sid
       AND date(c.valid_to_date) = '9999-12-31'
  ;

