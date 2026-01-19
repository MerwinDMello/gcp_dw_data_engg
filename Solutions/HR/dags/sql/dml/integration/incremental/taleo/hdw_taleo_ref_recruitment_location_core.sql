BEGIN
DECLARE DUP_COUNT INT64;
BEGIN TRANSACTION;
  
  MERGE INTO {{ params.param_hr_core_dataset_name }}.ref_recruitment_location AS tgt USING (
    SELECT
        location_num,
        level_num,
        location_name,
        location_code_text,
        work_location_code_text,
        addr_sid,
        source_system_code,
        dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.ref_recruitment_location_wrk
  ) AS src
  ON tgt.location_num = src.location_num
   AND tgt.source_system_code = src.source_system_code
     WHEN MATCHED THEN UPDATE SET level_num = src.level_num, location_name = src.location_name, location_code_text = src.location_code_text, work_location_code_text = src.work_location_code_text, addr_sid = src.addr_sid, dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND)
     WHEN NOT MATCHED BY TARGET THEN INSERT (location_num, level_num, location_name, location_code_text, work_location_code_text, addr_sid, source_system_code, dw_last_update_date_time) VALUES (src.location_num, src.level_num, src.location_name, src.location_code_text, src.work_location_code_text, src.addr_sid, src.source_system_code, DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND))
  ;
  SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            location_num
        from {{ params.param_hr_core_dataset_name }}.ref_recruitment_location
        group by location_num
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.ref_recruitment_location');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
