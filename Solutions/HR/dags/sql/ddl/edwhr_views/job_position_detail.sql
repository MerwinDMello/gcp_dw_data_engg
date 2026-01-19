/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_position_detail AS SELECT
      a.position_sid,
      a.position_type_id,
      a.position_detail_code,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.eff_to_date,
      a.detail_value_alphanumeric_text,
      a.detail_value_num,
      a.detail_value_date,
      a.lawson_object_id,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_position_detail AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

