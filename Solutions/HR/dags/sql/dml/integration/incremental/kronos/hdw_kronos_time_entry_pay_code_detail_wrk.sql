/*  Truncate Worktable Table     */
BEGIN
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;
  TRUNCATE TABLE {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk;

/*  Load Work Table with working Data */
  INSERT INTO {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk (employee_num, kronos_num, clock_library_code, kronos_pay_code_seq_num, valid_from_date, valid_to_date, kronos_pay_code, rounded_clocked_hour_num, pay_summary_group_code, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        time_entry_pay_code_detail_stg.employee_num,
        time_entry_pay_code_detail_stg.kronos_time_id AS kronos_num,
        time_entry_pay_code_detail_stg.kronos_clock_library AS clock_library_code,
        time_entry_pay_code_detail_stg.seq_kronos_pay_code_seq_num AS kronos_pay_code_seq_num,
        current_ts AS valid_from_date,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        time_entry_pay_code_detail_stg.kronos_pay_code,
        time_entry_pay_code_detail_stg.hours AS rounded_clocked_hour_num,
        time_entry_pay_code_detail_stg.pay_summary_group AS pay_summary_group_code,
        time_entry_pay_code_detail_stg.hr_company AS lawson_company_num,
        lpad(time_entry_pay_code_detail_stg.process_level, 5, '0') AS process_level_code,
        time_entry_pay_code_detail_stg.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_stg as time_entry_pay_code_detail_stg;
END
;