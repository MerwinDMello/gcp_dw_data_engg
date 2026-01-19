/***************************************************************************************
S E C U R I T Y   V I E W
****************************************************************************************/

  CREATE OR REPLACE VIEW {{ params.param_hr_views_dataset_name }}.time_entry_pay_code_detail AS SELECT
      a.employee_num,
      a.kronos_num,
      a.clock_library_code,
      a.kronos_pay_code_seq_num,
      a.valid_from_date,
      a.valid_to_date,
      a.kronos_pay_code,
      a.rounded_clocked_hour_num,
      a.pay_summary_group_code,
      a.lawson_company_num,
      a.process_level_code,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_hr_base_views_dataset_name }}.time_entry_pay_code_detail AS a
      INNER JOIN {{ params.param_hr_base_views_dataset_name }}.hr_secref_process_level AS c ON a.process_level_code = c.process_level_code
       AND a.lawson_company_num = c.lawson_company_num
       AND c.user_id = session_user()
  ;

