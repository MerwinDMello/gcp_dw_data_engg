BEGIN
DECLARE DUP_COUNT INT64;
DECLARE current_ts datetime;
SET current_ts = timestamp_trunc(current_datetime('US/Central'), SECOND) ;
BEGIN TRANSACTION;


  MERGE INTO {{ params.param_hr_core_dataset_name }}.pay_code_pay_summary_group_crosswalk AS tgt USING (
    SELECT * FROM (SELECT
        trim(pcpsgc.kronos_clock_library) AS clock_library_code,
        trim(pcpsgc.pay_code) AS kronos_pay_code,
        trim(pcpsgc.pay_code_desc) AS kronos_pay_code_desc,
        trim(pcpsgc.pay_summary_group) AS lawson_pay_summary_group_code,
        trim(pcpsgc.kronos_payroll_interface_code) AS lawson_pay_code,
        pcpsgc.source_system_code,
        current_ts AS dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.pay_code_pay_summary_group_crosswalk_stg AS pcpsgc
      GROUP BY 1, 2, 3, 4, 5, 6, 7)X
	  QUALIFY row_number() OVER (PARTITION BY X.clock_library_code, X.kronos_pay_code ORDER BY X.source_system_code DESC) = 1
  ) AS stg
  ON tgt.clock_library_code = stg.clock_library_code
   AND tgt.kronos_pay_code = stg.kronos_pay_code
     WHEN MATCHED THEN UPDATE SET kronos_pay_code_desc = stg.kronos_pay_code_desc, lawson_pay_summary_group_code = stg.lawson_pay_summary_group_code, lawson_pay_code = stg.lawson_pay_code, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
     WHEN NOT MATCHED BY TARGET THEN INSERT (clock_library_code, kronos_pay_code, kronos_pay_code_desc, lawson_pay_summary_group_code, lawson_pay_code, source_system_code, dw_last_update_date_time) VALUES (stg.clock_library_code, stg.kronos_pay_code, stg.kronos_pay_code_desc, stg.lawson_pay_summary_group_code, stg.lawson_pay_code, stg.source_system_code, stg.dw_last_update_date_time);

  /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select Clock_Library_Code ,Kronos_Pay_Code
        from {{ params.param_hr_core_dataset_name }}.pay_code_pay_summary_group_crosswalk
        group by Clock_Library_Code ,Kronos_Pay_Code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: edwhr.pay_code_pay_summary_group_crosswalk');
    ELSE
      COMMIT TRANSACTION;
    END IF;



END;
