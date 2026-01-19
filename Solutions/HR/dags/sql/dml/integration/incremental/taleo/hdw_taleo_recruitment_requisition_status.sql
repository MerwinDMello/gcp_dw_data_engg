  BEGIN
  DECLARE dup_count INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);


  BEGIN TRANSACTION;
  UPDATE {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status AS tgt 
  SET valid_to_date = current_dt - INTERVAL 1 SECOND, 
  dw_last_update_date_time = TIMESTAMP_TRUNC(CURRENT_DATETIME('US/Central'), SECOND) 
  FROM {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_status_wrk AS stg 
  WHERE tgt.recruitment_requisition_sid = stg.recruitment_requisition_sid
   AND (trim(CAST(coalesce(tgt.requisition_status_id, -999) as STRING)) <> trim(CAST(coalesce(stg.requisition_status_id, -999) as STRING))
   OR coalesce(trim(tgt.source_system_code), '') <> coalesce(trim(stg.source_system_code), ''))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

  INSERT INTO {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status (recruitment_requisition_sid, valid_from_date, valid_to_date, requisition_status_id, source_system_code, dw_last_update_date_time)
    SELECT
        stg.recruitment_requisition_sid,
        current_dt,
        DATETIME("9999-12-31 23:59:59"),
        stg.requisition_status_id,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_status_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_status AS tgt ON stg.recruitment_requisition_sid = tgt.recruitment_requisition_sid
         AND trim(CAST(coalesce(tgt.requisition_status_id, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_status_id, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), '') = coalesce(trim(stg.source_system_code), '')
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
      WHERE tgt.requisition_status_id IS NULL
  ;
/*
--  RETIRE RECORD ON 2ND RETIRE LOGIC 
UPDATE EDWHR.RECRUITMENT_REQUISITION_STATUS TGT
SET 
Valid_To_Date = VALID_FROM_DATE -1 
,DW_LAST_UPDATE_DATE_TIME = CURRENT_TIMESTAMP(0)
WHERE 
TGT.VALID_TO_DATE = DATETIME("9999-12-31 23:59:59")
AND 
  (TGT.RECRUITMENT_REQUISITION_STATUS_NUM) NOT IN ( SEL RECRUITMENT_REQUISITION_STATUS_NUM FROM  EDWHR_STAGING.RECRUITMENT_REQUISITION_STATUS_WRK  )
;*/


    /* Test Unique Index constraint set in Terdata */
    SET dup_count = ( 
        select count(*)
        from (
        select
            recruitment_requisition_sid ,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status
        group by recruitment_requisition_sid ,valid_from_date
        having count(*) > 1
        )
    );
    IF dup_count <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_requisition_status');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;