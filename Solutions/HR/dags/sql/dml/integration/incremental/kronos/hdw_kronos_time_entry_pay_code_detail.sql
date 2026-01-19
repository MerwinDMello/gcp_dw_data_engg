BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail AS tgt SET valid_to_date = DATE_SUB(current_ts , INTERVAL 1 SECOND), dw_last_update_date_time = current_ts FROM {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk AS stg WHERE tgt.employee_num = stg.employee_num
   AND tgt.kronos_num = stg.kronos_num
   AND tgt.clock_library_code = stg.clock_library_code
   AND tgt.kronos_pay_code_seq_num = stg.kronos_pay_code_seq_num
   AND (upper(trim(coalesce(nullif(ltrim(tgt.kronos_pay_code),''), 'X'))) <> upper(trim(coalesce(nullif(ltrim(stg.kronos_pay_code),''), 'X')))
   OR coalesce(tgt.rounded_clocked_hour_num, 0) <> coalesce(stg.rounded_clocked_hour_num, 0)
   OR upper(trim(coalesce(nullif(ltrim(tgt.pay_summary_group_code),''), 'X'))) <> upper(trim(coalesce(nullif(ltrim(stg.pay_summary_group_code),''), 'X')))
   OR trim(CAST(coalesce(tgt.lawson_company_num, 0) as STRING)) <> trim(CAST(coalesce(stg.lawson_company_num, 0) as STRING))
   OR trim(CAST(coalesce(lpad(tgt.process_level_code, 5, '0'), '0') as STRING)) <> trim(CAST(coalesce(lpad(stg.process_level_code, 5, '0'), '0') as STRING)))
 
   AND tgt.valid_to_date =  DATETIME(TIMESTAMP "9999-12-31 23:59:59+00");

   
 /*  Insering into edwhr_copy.time_entry_pay_code_detail*/
INSERT INTO {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail (employee_num, kronos_num, clock_library_code, kronos_pay_code_seq_num, valid_from_date, valid_to_date, kronos_pay_code, rounded_clocked_hour_num, pay_summary_group_code, lawson_company_num, process_level_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.employee_num,
        stg.kronos_num,
        stg.clock_library_code,
        stg.kronos_pay_code_seq_num,
        current_ts,
        stg.valid_to_date,
        stg.kronos_pay_code,
        stg.rounded_clocked_hour_num,
        stg.pay_summary_group_code,
        stg.lawson_company_num,
        stg.process_level_code,
        stg.source_system_code,
        current_ts
      FROM
        {{ params.param_hr_stage_dataset_name }}.time_entry_pay_code_detail_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail AS tgt ON tgt.employee_num = stg.employee_num
         AND tgt.kronos_num = stg.kronos_num
         AND tgt.clock_library_code = stg.clock_library_code
         AND tgt.kronos_pay_code_seq_num = stg.kronos_pay_code_seq_num
         AND DATE(tgt.valid_to_date) =  "9999-12-31"
      WHERE tgt.employee_num IS NULL
       AND tgt.kronos_num IS NULL
       AND tgt.clock_library_code IS NULL
       AND tgt.kronos_pay_code_seq_num IS NULL
  ;
  
  
  
 /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Employee_Num ,Kronos_Num ,Clock_Library_Code ,Kronos_Pay_Code_Seq_Num ,Valid_From_Date
        from {{ params.param_hr_core_dataset_name }}.time_entry_pay_code_detail
        group by Employee_Num ,Kronos_Num ,Clock_Library_Code ,Kronos_Pay_Code_Seq_Num ,Valid_From_Date		
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr_copy.time_entry_pay_code_detail');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END
;