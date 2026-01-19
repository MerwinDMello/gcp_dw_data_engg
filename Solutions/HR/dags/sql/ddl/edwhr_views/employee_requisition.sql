/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_requisition AS SELECT
      a.employee_sid,
      a.requisition_sid,
      a.action_type_code,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.action_code,
      a.user_id_text,
      a.work_unit_num,
      a.lawson_company_num,
      a.process_level_code,
      a.requisition_num,
      a.employee_num,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_requisition AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

