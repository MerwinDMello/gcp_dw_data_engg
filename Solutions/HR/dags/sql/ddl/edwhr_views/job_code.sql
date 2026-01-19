/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_code AS SELECT
      a.job_code_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.eff_from_date,
      a.job_class_sid,
      a.hr_company_sid,
      a.lawson_company_num,
      a.job_code,
      a.job_code_desc,
      a.active_dw_ind,
      a.eeo_category_code,
      a.eeo_code,
      a.process_level_code,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_code AS a
    WHERE (a.process_level_code, a.lawson_company_num) IN(
      SELECT AS STRUCT
          process_level_code,
          lawson_company_num
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
        WHERE user_id = session_user()
    )
  ;

