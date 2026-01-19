/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_class AS SELECT
      a.job_class_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_company_sid,
      a.lawson_company_num,
      a.job_class_code,
      a.job_class_desc,
      a.process_level_code,
      a.active_dw_ind,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_class AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

