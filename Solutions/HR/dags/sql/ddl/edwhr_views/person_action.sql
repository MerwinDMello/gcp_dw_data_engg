/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.person_action AS SELECT
      a.person_action_sid,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.action_code,
      a.employee_sid,
      a.applicant_sid,
      a.employee_num,
      a.applicant_num,
      a.action_type_code,
      a.action_sequence_num,
      a.action_from_date,
      a.action_to_date,
      a.requisition_sid,
      a.action_reason_text,
      a.person_action_update_sid,
      a.person_action_flag,
      a.action_last_update_date,
      a.active_dw_ind,
      a.delete_ind,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.person_action AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

