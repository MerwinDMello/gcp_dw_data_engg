BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;

BEGIN TRANSACTION;
UPDATE
  {{ params.param_hr_core_dataset_name }}.time_entry AS tgt
SET
  valid_to_date = current_ts - INTERVAL 1 SECOND, 
  dw_last_update_date_time =  TIMESTAMP_TRUNC(current_datetime('US/Central'), SECOND)
FROM
  {{ params.param_hr_stage_dataset_name }}.time_entry_wrk AS stg
WHERE
  tgt.employee_num = stg.employee_num
  AND tgt.kronos_num = stg.kronos_num
  AND tgt.clock_library_code = stg.clock_library_code
  AND (UPPER(TRIM(COALESCE(tgt.clock_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.clock_code, 'X')))
    OR COALESCE(tgt.clock_in_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.clock_in_time, DATETIME '1900-01-01 00:00:00')
    OR COALESCE(tgt.clock_out_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.clock_out_time, DATETIME '1900-01-01 00:00:00')
    OR TRIM(CAST(COALESCE(tgt.clocked_hour_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(stg.clocked_hour_num, 0) AS STRING))
    OR COALESCE(tgt.rounded_clock_in_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.rounded_clock_in_time, DATETIME '1900-01-01 00:00:00')
    OR COALESCE(tgt.rounded_clock_out_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.rounded_clock_out_time, DATETIME '1900-01-01 00:00:00')
    OR TRIM(CAST(COALESCE(tgt.rounded_clocked_hour_num, 0) AS STRING)) <> TRIM(COALESCE(CAST(stg.rounded_clocked_hour_num AS STRING), 'X'))
    OR COALESCE(tgt.time_approval_date_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.time_approval_date_time, DATETIME '1900-01-01 00:00:00')
    OR UPPER(TRIM(COALESCE(tgt.time_approver_34_login_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.time_approver_34_login_code, 'X')))
    OR COALESCE(tgt.scheduled_shift_date_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.scheduled_shift_date_time, DATETIME '1900-01-01 00:00:00')
    OR COALESCE(tgt.pay_period_start_date_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.pay_period_start_date_time, DATETIME '1900-01-01 00:00:00')
    OR COALESCE(tgt.pay_period_end_date_time, DATETIME '1900-01-01 00:00:00') <> COALESCE(stg.pay_period_end_date_time, DATETIME '1900-01-01 00:00:00')
    OR UPPER(TRIM(COALESCE(tgt.pay_type_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.pay_type_code, 'X')))
    OR UPPER(TRIM(COALESCE(tgt.long_meal_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.long_meal_code, 'X')))
    OR UPPER(TRIM(COALESCE(tgt.other_dept_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.other_dept_code, 'X')))
    OR UPPER(TRIM(COALESCE(tgt.out_of_pay_period_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.out_of_pay_period_code, 'X')))
    OR UPPER(TRIM(COALESCE(tgt.short_meal_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.short_meal_code, 'X')))
    OR UPPER(TRIM(COALESCE(tgt.dept_code, 'X'))) <> UPPER(TRIM(COALESCE(stg.dept_code, 'X')))
    OR TRIM(CAST(COALESCE(tgt.lawson_company_num, 0) AS STRING)) <> TRIM(CAST(COALESCE(stg.lawson_company_num, 0) AS STRING))
    OR UPPER(TRIM(COALESCE(lpad(tgt.process_level_code,5,'0'), 'X'))) <> UPPER(TRIM(COALESCE(stg.process_level_code, 'X')))
    OR COALESCE(TRIM(tgt.process_level_code) , 'X') <> COALESCE(TRIM(stg.process_level_code) , 'X')
    OR UPPER(COALESCE(tgt.source_system_code, '')) <> UPPER(COALESCE(stg.source_system_code, ''))
    OR UPPER(COALESCE(tgt.posted_ind, 'X')) <> UPPER(COALESCE(stg.posted_ind, 'X')))
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59"); 

/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_Num,
      Kronos_Num,
      Clock_Library_Code,
      Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.time_entry
    GROUP BY
      Employee_Num,
      Kronos_Num,
      Clock_Library_Code,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.time_entry');
  ELSE
COMMIT TRANSACTION;
END IF
  ;


BEGIN TRANSACTION;
INSERT INTO
  {{ params.param_hr_core_dataset_name }}.time_entry (employee_num,
    kronos_num,
    clock_library_code,
    valid_from_date,
    valid_to_date,
    clock_code,
    clock_in_time,
    clock_out_time,
    clocked_hour_num,
    rounded_clock_in_time,
    rounded_clock_out_time,
    rounded_clocked_hour_num,
    time_approval_date_time,
    time_approver_34_login_code,
    scheduled_shift_date_time,
    pay_period_start_date_time,
    pay_period_end_date_time,
    pay_type_code,
    long_meal_code,
    other_dept_code,
    out_of_pay_period_code,
    short_meal_code,
    dept_code,
    lawson_company_num,
    process_level_code,
    source_system_code,
    posted_ind,
    dw_last_update_date_time)
SELECT
  stg.employee_num,
  stg.kronos_num,
  stg.clock_library_code,
  current_ts AS valid_from_date,
  DATETIME("9999-12-31 23:59:59") AS valid_to_date,
  stg.clock_code,
  stg.clock_in_time,
  stg.clock_out_time,
  stg.clocked_hour_num,
  stg.rounded_clock_in_time,
  stg.rounded_clock_out_time,
  stg.rounded_clocked_hour_num,
  stg.time_approval_date_time,
  stg.time_approver_34_login_code,
  stg.scheduled_shift_date_time,
  stg.pay_period_start_date_time,
  stg.pay_period_end_date_time,
  stg.pay_type_code,
  stg.long_meal_code,
  stg.other_dept_code,
  stg.out_of_pay_period_code,
  stg.short_meal_code,
  stg.dept_code,
  stg.lawson_company_num,
  stg.process_level_code,
  stg.source_system_code,
  stg.posted_ind,
  stg.dw_last_update_date_time
FROM
  {{ params.param_hr_stage_dataset_name }}.time_entry_wrk AS stg
LEFT OUTER JOIN
  {{ params.param_hr_core_dataset_name }}.time_entry AS tgt
ON
  tgt.employee_num = stg.employee_num
  AND tgt.kronos_num = stg.kronos_num
  AND tgt.clock_library_code = stg.clock_library_code
  AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
WHERE
  tgt.employee_num IS NULL
  AND tgt.kronos_num IS NULL
  AND tgt.clock_library_code IS NULL ; 
  
/* Test Unique Primary Index constraint set in Terdata */
SET
  DUP_COUNT = (
  SELECT
    COUNT(*)
  FROM (
    SELECT
      Employee_Num,
      Kronos_Num,
      Clock_Library_Code,
      Valid_From_Date
    FROM
      {{ params.param_hr_core_dataset_name }}.time_entry
    GROUP BY
      Employee_Num,
      Kronos_Num,
      Clock_Library_Code,
      Valid_From_Date
    HAVING
      COUNT(*) > 1 ) );
IF
  DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION; RAISE
USING
  MESSAGE = CONCAT('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.time_entry');
  ELSE
COMMIT TRANSACTION;
END IF
  ;
END
  ;