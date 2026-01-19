/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_flu_vaccination_consent AS SELECT
      a.personnel_sid,
      a.flu_season_year_text,
      a.employee_sid,
      a.job_class_sid,
      a.location_code,
      a.department_sid,
      a.employee_type_code,
      a.employee_3_4_login_code,
      a.status_code,
      a.consent_ind,
      a.consent_response_desc,
      a.consent_location_desc,
      a.decline_reason_desc,
      a.last_update_date,
      a.personnel_type_code,
      a.supervisor_name,
      a.email_address,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_flu_vaccination_consent AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

