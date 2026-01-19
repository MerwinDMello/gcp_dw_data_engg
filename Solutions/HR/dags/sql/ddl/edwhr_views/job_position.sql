/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.job_position AS SELECT
      a.position_sid,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.link_supervisor_sid,
      a.supervisor_sid,
      a.job_code_sid,
      a.hr_company_sid,
      a.position_change_reason_code,
      a.location_code,
      a.pay_grade_code,
      a.position_code,
      a.account_unit_num,
      a.gl_company_num,
      a.overtime_plan_code,
      a.overtime_exempt_ind,
      a.position_code_desc,
      a.company_position_eff_from_date,
      a.company_position_eff_to_date,
      a.pay_grade_schedule_code,
      a.pay_step_num,
      a.union_code,
      a.shift_num,
      a.eff_to_date,
      a.lawson_object_id,
      a.schedule_work_code,
      a.user_level_code,
      a.dept_sid,
      a.lawson_company_num,
      a.process_level_code,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.job_position AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

