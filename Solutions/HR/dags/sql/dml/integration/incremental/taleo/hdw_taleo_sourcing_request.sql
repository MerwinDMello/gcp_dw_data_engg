
/*  CLOSE THE PREVIOUS RECORDS FROM TARGET TABLE FOR SAME KEY FOR ANY CHANGES  */
/*  INSERT THE NEW RECORDS/CHNAGES INTO THE TARGET TABLE  */
/* BEGIN TRANSACTION BLOCK STARTS HERE */
BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = datetime_trunc(current_datetime('US/Central'), second);

  BEGIN TRANSACTION;

  UPDATE {{ params.param_hr_core_dataset_name }}.sourcing_request AS tgt 
    SET valid_to_date = current_dt - INTERVAL 1 SECOND, 
    dw_last_update_date_time = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) 
  FROM {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk AS stg WHERE tgt.sourcing_request_sid = stg.sourcing_request_sid
   AND (trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.job_board_id, -999) as STRING)) <> trim(CAST(coalesce(stg.job_board_id, -999) as STRING))
   OR trim(CAST(coalesce(tgt.source_request_status_id, -999) as STRING)) <> trim(CAST(coalesce(stg.source_request_status_id, -999) as STRING))
   OR trim(CAST(coalesce(tgt.job_board_type_id, -999) as STRING)) <> trim(CAST(coalesce(stg.job_board_type_id, -999) as STRING))
   OR trim(CAST(coalesce(tgt.posting_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.posting_date, DATE '1900-01-01') as STRING))
   OR trim(CAST(coalesce(tgt.unposting_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.unposting_date, DATE '1900-01-01') as STRING))
   OR trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) <> trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
   OR trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) <> trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
   OR coalesce(trim(tgt.source_system_code), 'XX') <> coalesce(trim(stg.source_system_code), 'XX'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.sourcing_request (sourcing_request_sid, valid_from_date, recruitment_requisition_sid, job_board_id, source_request_status_id, job_board_type_id, valid_to_date, posting_date, unposting_date, creation_date, requisition_num, source_system_code, dw_last_update_date_time)
    SELECT
        stg.sourcing_request_sid,
        current_dt AS valid_from_date,
        stg.recruitment_requisition_sid,
        stg.job_board_id,
        stg.source_request_status_id,
        stg.job_board_type_id,
        DATETIME("9999-12-31 23:59:59") AS valid_to_date,
        stg.posting_date,
        stg.unposting_date,
        stg.creation_date,
        stg.requisition_num,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.sourcing_request_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.sourcing_request AS tgt ON stg.sourcing_request_sid = tgt.sourcing_request_sid
         AND trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_board_id, -999) as STRING)) = trim(CAST(coalesce(stg.job_board_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.source_request_status_id, -999) as STRING)) = trim(CAST(coalesce(stg.source_request_status_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_board_type_id, -999) as STRING)) = trim(CAST(coalesce(stg.job_board_type_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.posting_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.posting_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.unposting_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.unposting_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.creation_date, DATE '1900-01-01') as STRING)) = trim(CAST(coalesce(stg.creation_date, DATE '1900-01-01') as STRING))
         AND trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), '') = coalesce(trim(stg.source_system_code), '')
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.sourcing_request_sid IS NULL
  ;
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            sourcing_request_sid, valid_from_date
        from {{ params.param_hr_core_dataset_name }}.sourcing_request
        group by sourcing_request_sid, valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.sourcing_request_wrk');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
