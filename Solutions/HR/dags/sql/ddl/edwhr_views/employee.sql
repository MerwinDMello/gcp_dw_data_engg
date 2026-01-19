/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee AS SELECT
      a.employee_sid,
      a.valid_from_date,
      a.valid_to_date,
      a.process_level_sid,
      a.dept_sid,
      a.location_code,
      a.pay_grade_code,
      a.union_code,
      a.user_level_code,
      a.lawson_company_num,
      a.process_level_code,
      a.employee_num,
      a.account_unit_num,
      a.gl_company_num,
      a.dept_code,
      a.employee_34_login_code,
      a.adjusted_hire_date,
      a.anniversary_date,
      a.termination_date,
      a.hire_date,
      a.new_hire_date,
      a.primary_facility_ind,
      a.fte_percent,
      a.pay_grade_schedule_code,
      a.pay_step_num,
      a.pay_rate_amt,
      a.security_key_text,
      a.remote_sw,
      a.active_dw_ind,
      a.delete_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee AS a
    WHERE (a.process_level_code, a.lawson_company_num) IN(
      SELECT AS STRUCT
          process_level_code,
          lawson_company_num
        FROM
          {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level
        WHERE user_id = session_user()
    )
  ;

