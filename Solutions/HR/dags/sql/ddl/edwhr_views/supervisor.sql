/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.supervisor AS SELECT
      a.supervisor_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.eff_from_date,
      a.employee_sid,
      a.employee_num,
      a.hr_company_sid,
      a.reporting_supervisor_sid,
      a.supervisor_code,
      a.supervisor_desc,
      a.officer_code,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.supervisor AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

