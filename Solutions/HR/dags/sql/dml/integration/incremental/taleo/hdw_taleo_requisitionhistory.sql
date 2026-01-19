BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);

  BEGIN TRANSACTION;
  
  UPDATE {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_history_wrk AS stg WHERE tgt.recruitment_requisition_sid = stg.recruitment_requisition_sid
   AND tgt.creation_date_time = stg.creation_date_time
   AND tgt.requisition_status_id = stg.requisition_status_id
   AND tgt.source_system_code = stg.source_system_code
   AND (coalesce(tgt.closed_date_time, DATETIME '1901-01-01 00:00:00') <> coalesce(stg.closed_date_time, DATETIME '1901-01-01 00:00:00')
   OR trim(CAST(coalesce(tgt.requisition_creator_user_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.requisition_creator_user_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.recruiter_owner_user_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitier_owner_user_sid, -999) as STRING)))
   AND tgt.valid_to_date = DATETIME('9999-12-31 23:59:59');

  INSERT INTO {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history (recruitment_requisition_sid, creation_date_time, requisition_status_id, valid_from_date, valid_to_date, closed_date_time, requisition_creator_user_sid, recruiter_owner_user_sid, source_system_code, dw_last_update_date_time)
    SELECT
        stg.recruitment_requisition_sid,
        stg.creation_date_time,
        stg.requisition_status_id,
        current_dt,
        DATETIME('9999-12-31 23:59:59'),
        stg.closed_date_time,
        stg.requisition_creator_user_sid,
        stg.recruitier_owner_user_sid,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.recruitment_requisition_history_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.recruitment_requisition_history AS tgt ON tgt.recruitment_requisition_sid = stg.recruitment_requisition_sid
         AND tgt.creation_date_time = stg.creation_date_time
         AND tgt.requisition_status_id = stg.requisition_status_id
         AND tgt.source_system_code = stg.source_system_code
         AND coalesce(tgt.closed_date_time, DATETIME('1901-01-01 00:00:00')) = coalesce(stg.closed_date_time, DATETIME('1901-01-01 00:00:00'))
         AND trim(CAST(coalesce(tgt.requisition_creator_user_sid, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_creator_user_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.recruiter_owner_user_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitier_owner_user_sid, -999) as STRING))
         AND tgt.valid_to_date = DATETIME('9999-12-31 23:59:59')
      WHERE tgt.recruitment_requisition_sid IS NULL
  ;

    /* Test Unique Primary Index constraint set in Terdata */
    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
          Recruitment_Requisition_SID ,Creation_Date_Time ,Requisition_Status_Id ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history
        group by Recruitment_Requisition_SID ,Creation_Date_Time ,Requisition_Status_Id ,Valid_From_Date  
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.recruitment_requisition_history');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;