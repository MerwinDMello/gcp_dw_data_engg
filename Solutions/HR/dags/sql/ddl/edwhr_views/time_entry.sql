/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.time_entry AS SELECT
      a.employee_num,
      a.kronos_num,
      a.clock_library_code,
      a.valid_from_date,
      a.valid_to_date,
      a.clock_code,
      a.clock_in_time,
      a.clock_out_time,
      a.clocked_hour_num,
      a.rounded_clock_in_time,
      a.rounded_clock_out_time,
      a.rounded_clocked_hour_num,
      a.time_approval_date_time,
      a.time_approver_34_login_code,
      a.scheduled_shift_date_time,
      a.pay_period_start_date_time,
      a.pay_period_end_date_time,
      a.pay_type_code,
      a.long_meal_code,
      a.other_dept_code,
      a.out_of_pay_period_code,
      a.short_meal_code,
      a.dept_code,
      a.posted_ind,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.time_entry AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

