/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.company_pay_schedule AS SELECT
      a.company_pay_schedule_sid,
      a.eff_from_date,
      a.valid_from_date,
      a.valid_to_date,
      a.hr_company_sid,
      a.lawson_company_num,
      a.pay_schedule_code,
      a.pay_schedule_flag,
      a.pay_schedule_eff_date,
      a.pay_schedule_desc,
      a.salary_class_flag,
      a.last_grade_sequence_num,
      a.pay_schedule_status_ind,
      a.currency_code,
      a.currency_nd,
      a.active_dw_ind,
      a.process_level_code,
      a.security_key_text,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.company_pay_schedule AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

