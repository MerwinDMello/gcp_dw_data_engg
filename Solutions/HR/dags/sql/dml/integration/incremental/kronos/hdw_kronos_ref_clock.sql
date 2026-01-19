BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
    /*  LOAD TARGET TABLE WITH STAGING DATA */
    MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_clock AS tgt USING (
        SELECT
            trim(rc.clock_code) AS clock_code,
            trim(rc.kronos_clock_library) AS clock_library_code,
            trim(rc.clock_desc) AS clock_desc,
            rc.source_system_code,
            timestamp_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
          FROM
            {{ params.param_hr_stage_dataset_name }}.ref_clock_stg AS rc
          GROUP BY 1, 2, 3, 4, 5
      ) AS stg
      ON tgt.clock_code = stg.clock_code
      AND tgt.clock_library_code = stg.clock_library_code
        WHEN MATCHED THEN UPDATE SET clock_desc = stg.clock_desc, source_system_code = stg.source_system_code, dw_last_update_date_time = stg.dw_last_update_date_time
        WHEN NOT MATCHED BY TARGET THEN INSERT (clock_code, clock_library_code, clock_desc, source_system_code, dw_last_update_date_time) VALUES (stg.clock_code, stg.clock_library_code, stg.clock_desc, stg.source_system_code, stg.dw_last_update_date_time)
      ;

    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Clock_Code ,Clock_Library_Code
        from {{ params.param_hr_core_dataset_name }}.ref_clock
        group by Clock_Code ,Clock_Library_Code
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table:ref_clock');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;