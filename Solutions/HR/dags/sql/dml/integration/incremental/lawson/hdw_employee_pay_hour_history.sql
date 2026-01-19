BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_ts datetime;
  SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
  
BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history AS tgt SET valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.employee_pay_hour_history_wrk AS stg WHERE upper(tgt.active_dw_ind) = 'Y'
   AND upper(tgt.source_system_code) = 'L'
   AND tgt.employee_sid = stg.employee_sid
   AND tgt.check_id = stg.check_id
   AND tgt.pay_summary_group_code = stg.pay_summary_group_code
   AND tgt.time_seq_id = stg.time_seq_id
   AND tgt.source_system_code = stg.source_system_code
   AND (coalesce(tgt.work_hour_amt,0 ) <> coalesce(stg.work_hour_amt, 0)
   OR coalesce(tgt.hourly_rate_amt, 0) <> coalesce(stg.hourly_rate_amt, 0)
   OR coalesce(tgt.wage_amt, 0) <> coalesce(stg.wage_amt, 0)
   OR trim(coalesce(tgt.process_level_code, '')) <> trim(coalesce(stg.process_level_code, ''))
   OR coalesce(tgt.transaction_date, parse_date('%Y-%m-%d', '1800-01-01')) <> coalesce(stg.transaction_date, parse_date('%Y-%m-%d', '1800-01-01'))
   OR trim(CAST(coalesce(tgt.dept_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.dept_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.payroll_year_num, -999) as STRING)) <> trim(CAST(coalesce(stg.payroll_year_num, -999) as STRING))
   OR upper(trim(coalesce(tgt.home_process_level_code, ''))) <> upper(trim(coalesce(stg.home_process_level_code, '')))
   OR upper(trim(coalesce(tgt.gl_account_num, ''))) <> upper(trim(coalesce(stg.gl_account_num, '')))
   OR upper(trim(coalesce(tgt.gl_sub_account_num, ''))) <> upper(trim(coalesce(stg.gl_sub_account_num, '')))
   OR trim(CAST(coalesce(tgt.gl_company_num, -999) as STRING)) <> trim(CAST(coalesce(stg.gl_company_num, -999) as STRING))
   OR upper(trim(coalesce(tgt.gl_sub_account_num, ''))) <> upper(trim(coalesce(stg.gl_sub_account_num, '')))
   OR coalesce(tgt.pay_period_end_date, parse_date('%Y-%m-%d', '1800-01-01')) <> coalesce(stg.pay_period_end_date, parse_date('%Y-%m-%d', '1800-01-01')));
/*  Insert the New Records into the Base Table  */
  INSERT INTO {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history (employee_sid, check_id, pay_summary_group_code, time_seq_id, valid_from_date, valid_to_date, employee_num, work_hour_amt, hourly_rate_amt, wage_amt, transaction_date, dept_sid, payroll_year_num, home_process_level_code, gl_account_num, gl_sub_account_num, gl_company_num, account_unit_num, pay_period_end_date, lawson_company_num, process_level_code, delete_ind, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_sid,
        stg.check_id,
        stg.pay_summary_group_code,
        stg.time_seq_id,
        current_ts,
        stg.valid_to_date,
        stg.employee_num,
        stg.work_hour_amt,
        stg.hourly_rate_amt,
        stg.wage_amt,
        stg.transaction_date,
        stg.dept_sid,
        stg.payroll_year_num,
        stg.home_process_level_code,
        stg.gl_account_num,
        stg.gl_sub_account_num,
        stg.gl_company_num,
        stg.account_unit_num,
        stg.pay_period_end_date,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.delete_ind,
        stg.active_dw_ind,
        stg.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.employee_pay_hour_history_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.employee_pay_hour_history AS tgt ON tgt.employee_sid = stg.employee_sid
         AND tgt.check_id = stg.check_id
         AND tgt.pay_summary_group_code = stg.pay_summary_group_code
         AND tgt.time_seq_id = stg.time_seq_id
         AND coalesce(tgt.work_hour_amt, 0) = coalesce(stg.work_hour_amt,0)
         AND coalesce(tgt.hourly_rate_amt, 0) = coalesce(stg.hourly_rate_amt,0)
         AND coalesce(tgt.wage_amt, 0) = coalesce(stg.wage_amt, 0)
         AND trim(coalesce(tgt.process_level_code, '')) = trim(coalesce(stg.process_level_code, ''))
         AND coalesce(tgt.transaction_date, parse_date('%Y-%m-%d', '1800-01-01')) = coalesce(stg.transaction_date, parse_date('%Y-%m-%d', '1800-01-01'))
         AND trim(CAST(coalesce(tgt.dept_sid, -999) as STRING)) = trim(CAST(coalesce(stg.dept_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.payroll_year_num, -999) as STRING)) = trim(CAST(coalesce(stg.payroll_year_num, -999) as STRING))
         AND upper(trim(coalesce(tgt.home_process_level_code, ''))) = upper(trim(coalesce(stg.home_process_level_code, '')))
         AND upper(trim(coalesce(tgt.gl_account_num, ''))) = upper(trim(coalesce(stg.gl_account_num, '')))
         AND upper(trim(coalesce(tgt.gl_sub_account_num, ''))) = upper(trim(coalesce(stg.gl_sub_account_num, '')))
         AND trim(CAST(coalesce(tgt.gl_company_num, -999) as STRING)) = trim(CAST(coalesce(stg.gl_company_num, -999) as STRING))
         AND upper(trim(coalesce(tgt.gl_sub_account_num, ''))) = upper(trim(coalesce(stg.gl_sub_account_num, '')))
         AND coalesce(tgt.pay_period_end_date, parse_date('%Y-%m-%d', '1800-01-01')) = coalesce(stg.pay_period_end_date, parse_date('%Y-%m-%d', '1800-01-01'))
         AND upper(tgt.active_dw_ind) = 'Y'
      WHERE tgt.employee_sid IS NULL
  ;
/*  UPDATE  DELETE_IND   */
  UPDATE {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history AS tgt SET delete_ind = emp.delete_ind, valid_to_date = current_ts - INTERVAL 1 SECOND, active_dw_ind = 'N', dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_num,
        employee.lawson_company_num,
        employee.delete_ind
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'D'
      GROUP BY 1, 2, 3
  ) AS emp WHERE tgt.employee_num = emp.employee_num
    and tgt.Source_System_Code = 'L'
   AND tgt.lawson_company_num = emp.lawson_company_num
   AND upper(tgt.delete_ind) = 'A';

  UPDATE {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history AS tgt SET delete_ind = emp.delete_ind, dw_last_update_date_time = current_ts FROM (
    SELECT
        employee.employee_num,
        employee.lawson_company_num,
        employee.delete_ind
      FROM
        {{ params.param_hr_base_views_dataset_name }}.employee
      WHERE upper(employee.delete_ind) = 'A'
      GROUP BY 1, 2, 3
  ) AS emp WHERE tgt.employee_num = emp.employee_num
    and tgt.Source_System_Code = 'L'
   AND tgt.lawson_company_num = emp.lawson_company_num
   AND upper(tgt.delete_ind) = 'D';

/* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Employee_SID ,Check_Id ,Pay_Summary_Group_Code ,Time_Seq_Id ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history
        group by Employee_SID, Check_Id ,Pay_Summary_Group_Code ,Time_Seq_Id ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.employee_pay_hour_history');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
