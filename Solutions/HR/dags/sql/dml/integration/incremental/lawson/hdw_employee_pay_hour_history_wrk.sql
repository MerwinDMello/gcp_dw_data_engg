BEGIN
DECLARE current_ts datetime;
SET current_ts = datetime_trunc(current_datetime('US/Central'), SECOND) ;
/*  Truncate Wrk Table      */
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.employee_pay_hour_history_wrk;

/*  Load Wrk Table with Today's records */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.employee_pay_hour_history_wrk (employee_sid, check_id, pay_summary_group_code, time_seq_id, valid_from_date, valid_to_date, employee_num, work_hour_amt, hourly_rate_amt, wage_amt, transaction_date, dept_sid, payroll_year_num, home_process_level_code, gl_account_num, gl_sub_account_num, gl_company_num, account_unit_num, pay_period_end_date, lawson_company_num, process_level_code, delete_ind, active_dw_ind, source_system_code, dw_last_update_date_time)
    SELECT
        coalesce(eid1.employee_sid, 0) AS employee_sid,
        cast(stg.check_id as INT64),
        trim(stg.pay_sum_grp) AS pay_summary_group_code,
        stg.time_seq AS time_seq_id,
        current_ts AS valid_from_date,
        datetime("9999-12-31 23:59:59") AS valid_to_date,
        stg.employee AS employee_num,
        stg.r_hours AS work_hour_amt,
        stg.rate AS hourly_rate_amt,
        stg.wage_amount AS wage_amt,
        stg.tr_date AS transaction_date,
        dept.dept_sid,
        stg.payroll_year AS payroll_year_num,
        stg.hm_process_lev AS home_process_level_code,
        cast(max(stg.dst_account) as string) AS gl_account_num,
        cast(max(stg.dst_sub_acct) as string) AS gl_sub_account_num,
        stg.dist_company AS gl_company_num,
        max(stg.dst_acct_unit) AS account_unit_num,
        stg.per_end_date AS pay_period_end_date,
        stg.company AS lawson_company_num,
        trim(stg.process_level) AS process_level_code,
        'A' AS delete_ind,
        'Y' AS active_dw_ind,
        'L' AS source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.prtime AS stg
        LEFT OUTER JOIN (
          SELECT
              employee.employee_sid,
              employee.employee_num,
              employee.lawson_company_num
            FROM
              {{ params.param_hr_base_views_dataset_name }}.employee
            WHERE date(employee.valid_to_date) = "9999-12-31"
             AND upper(employee.source_system_code) = 'L'
            GROUP BY 1, 2, 3
        ) AS eid1 ON stg.employee = eid1.employee_num
         AND stg.company = eid1.lawson_company_num
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.department AS dept ON stg.department = dept.dept_code
         AND stg.process_level = dept.process_level_code
         AND stg.company = dept.lawson_company_num
         AND date(dept.valid_to_date) = "9999-12-31"
      GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, stg.dst_account, stg.dst_sub_acct, 17, upper(stg.dst_acct_unit), 19, 20, 21, 22, 23, 24, 25
  ;
END;