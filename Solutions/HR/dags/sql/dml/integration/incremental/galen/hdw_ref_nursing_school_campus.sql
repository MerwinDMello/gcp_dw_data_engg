BEGIN
  DECLARE DUP_COUNT INT64;

  BEGIN TRANSACTION;
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_nursing_school_campus AS tgt USING (
    SELECT
        campus_id,
        campus_name,
        campus_code,
        nursing_school_id,
        addr_sid,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_nursing_school_campus_wrk
  ) AS src
  ON tgt.campus_id = src.campus_id
     WHEN MATCHED THEN UPDATE SET campus_name = src.campus_name, campus_code = src.campus_code, addr_sid = src.addr_sid, dw_last_update_date_time = datetime_trunc(current_datetime('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (campus_id, campus_name, campus_code, nursing_school_id, addr_sid, source_system_code, dw_last_update_date_time) VALUES (src.campus_id, src.campus_name, src.campus_code, src.nursing_school_id, src.addr_sid, src.source_system_code, src.dw_last_update_date_time)
  ;
      SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           campus_id
        from {{ params.param_hr_core_dataset_name }}.ref_nursing_school_campus
        group by campus_id
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat( 'Duplicates are not allowed in the table: ref_nursing_school_campus' );
    ELSE
      COMMIT TRANSACTION;
    END IF;
    END;