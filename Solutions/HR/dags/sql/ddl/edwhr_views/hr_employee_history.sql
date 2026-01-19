/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.hr_employee_history AS SELECT
      a.employee_sid,
      a.lawson_element_num,
      a.hr_employee_element_date,
      a.lawson_object_id,
      a.sequence_num,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_employee_value_num,
      a.hr_employee_value_alphanumeric_text,
      a.hr_employee_value_date,
      a.action_object_identifier,
      a.user_3_4_login_code,
      a.data_type_flag,
      a.position_level_sequence_num,
      a.last_update_date,
      a.last_update_time,
      a.employee_num,
      a.lawson_company_num,
      a.process_level_code,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.hr_employee_history AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

