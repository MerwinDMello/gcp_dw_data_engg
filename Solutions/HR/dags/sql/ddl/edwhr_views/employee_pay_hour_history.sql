/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.employee_pay_hour_history AS SELECT
      a.employee_sid,
      a.check_id,
      a.pay_summary_group_code,
      a.time_seq_id,
      a.valid_from_date,
      a.valid_to_date,
      a.employee_num,
      a.work_hour_amt,
      a.hourly_rate_amt,
      a.wage_amt,
      a.transaction_date,
      a.dept_sid,
      a.payroll_year_num,
      a.home_process_level_code,
      a.gl_account_num,
      a.gl_sub_account_num,
      a.gl_company_num,
      a.account_unit_num,
      a.pay_period_end_date,
      a.lawson_company_num,
      a.process_level_code,
      a.delete_ind,
      a.active_dw_ind,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.employee_pay_hour_history AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

