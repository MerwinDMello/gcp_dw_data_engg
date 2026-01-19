/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.status AS SELECT
      a.status_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_company_sid,
      a.lawson_company_num,
      a.status_type_code,
      a.status_code,
      a.status_desc,
      a.active_dw_ind,
      a.process_level_code,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.status AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

