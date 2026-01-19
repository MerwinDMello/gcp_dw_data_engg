/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_code_detail AS SELECT
      a.employee_sid,
      a.employee_type_code,
      a.employee_sw,
      a.employee_code,
      a.employee_code_subject_code,
      a.employee_code_seq_num,
      a.valid_from_date,
      a.employee_num,
      a.acquired_date,
      a.renew_date,
      a.certification_renew_date,
      a.license_num_text,
      a.proficiency_level_text,
      a.verified_ind,
      a.employee_code_detail_text,
      a.company_sponsored_ind,
      a.skill_source_code,
      a.lawson_company_num,
      a.process_level_code,
      a.state_code,
      a.valid_to_date,
      a.active_dw_ind,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_code_detail AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

