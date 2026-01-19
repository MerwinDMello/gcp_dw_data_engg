/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.requisition AS SELECT
      a.requisition_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_company_sid,
      a.application_status_sid,
      a.lawson_company_num,
      a.process_level_code,
      a.location_code,
      a.requisition_num,
      a.requisition_desc,
      a.requisition_eff_date,
      a.requisition_open_date,
      a.requisition_closed_date,
      a.requisition_origination_date,
      a.originator_login_3_4_code,
      a.position_needed_date,
      a.job_opening_cnt,
      a.open_fte_percent,
      a.filled_fte_percent,
      a.last_update_date,
      a.replacement_employee_num,
      a.replacement_employee_sid,
      a.replacement_flag,
      a.special_requirement_text,
      a.work_schedule_code,
      a.union_code,
      a.active_dw_ind,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.requisition AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

