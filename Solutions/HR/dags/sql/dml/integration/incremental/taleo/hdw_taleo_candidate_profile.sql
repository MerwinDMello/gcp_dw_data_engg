BEGIN
  DECLARE DUP_COUNT INT64;
  DECLARE current_dt DATETIME;
  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'),SECOND);

  BEGIN TRANSACTION;

/* Collecting Stats on work table/s */
/*  Close the previous records from Target table for same key for any Changes  */
/*  Insert the New Records/Chnages into the Target Table  */
/* Begin Transaction Block Starts Here */

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_profile AS tgt SET valid_to_date = current_dt - INTERVAL 1 SECOND, dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk AS stg WHERE tgt.candidate_profile_sid = stg.candidate_profile_sid
   AND tgt.source_system_code = stg.source_system_code
   AND (trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.candidate_profile_num, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_profile_num, -999) as STRING))
   OR coalesce(tgt.submission_date, DATE '1901-01-01') <> coalesce(stg.submission_date, DATE '1901-01-01')
   OR coalesce(tgt.submission_date_time , CAST('1901-01-01 00:00:00' AS datetime)) <> coalesce(stg.submission_date_time , CAST('1901-01-01 00:00:00' AS datetime))
   OR coalesce(tgt.completion_date, DATE '1901-01-01') <> coalesce(stg.completion_date, DATE '1901-01-01')
   OR coalesce(tgt.completion_date_time , CAST('1901-01-01 00:00:00' AS datetime)) <> coalesce(stg.completion_date_time , CAST('1901-01-01 00:00:00' AS datetime))
   OR coalesce(tgt.creation_date, DATE '1901-01-01') <> coalesce(stg.creation_date, DATE '1901-01-01')
   OR coalesce(tgt.creation_date_time , CAST('1901-01-01 00:00:00' AS datetime)) <> coalesce(stg.creation_date_time , CAST('1901-01-01 00:00:00' AS datetime))
   OR trim(CAST(coalesce(tgt.recruitment_source_id, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_source_id, -999) as STRING))
   OR trim(CAST(coalesce(tgt.recruitment_source_auto_filled_sw, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_source_auto_filled_sw, -999) as STRING))
   OR trim(CAST(coalesce(tgt.job_application_num, -999) as STRING)) <> trim(CAST(coalesce(stg.job_application_num, -999) as STRING))
   OR trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) <> trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
   OR trim(CAST(coalesce(tgt.candidate_num, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_num, -999) as STRING)))
   AND DATE(tgt.valid_to_date) = '9999-12-31';

  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_profile (candidate_profile_sid, valid_from_date, candidate_sid, profile_medium_id, candidate_profile_num, submission_date, submission_date_time, completion_date, completion_date_time, creation_date, creation_date_time, recruitment_source_id, recruitment_source_auto_filled_sw, job_application_num, requisition_num, candidate_num, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.candidate_profile_sid,
        current_dt,
        stg.candidate_sid,
        stg.profile_medium_id,
        stg.candidate_profile_num,
        stg.submission_date,
		stg.submission_date_time,
        stg.completion_date,
		stg.completion_date_time,
        stg.creation_date,
		stg.creation_date_time,
        stg.recruitment_source_id,
        stg.recruitment_source_auto_filled_sw,
        stg.job_application_num,
        stg.requisition_num,
        stg.candidate_num,
        DATETIME('9999-12-31 23:59:59'),
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_profile_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_profile AS tgt ON stg.candidate_profile_sid = tgt.candidate_profile_sid
         AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.candidate_profile_num, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_profile_num, -999) as STRING))
         AND coalesce(tgt.submission_date, DATE '1901-01-01') = coalesce(stg.submission_date, DATE '1901-01-01')
		 AND coalesce(tgt.submission_date_time , CAST('1901-01-01 00:00:00' AS datetime)) = coalesce(stg.submission_date_time , CAST('1901-01-01 00:00:00' AS datetime))
         AND coalesce(tgt.completion_date, DATE '1901-01-01') = coalesce(stg.completion_date, DATE '1901-01-01')
         AND coalesce(tgt.completion_date_time , CAST('1901-01-01 00:00:00' AS datetime)) = coalesce(stg.completion_date_time , CAST('1901-01-01 00:00:00' AS datetime))
		 AND coalesce(tgt.creation_date, DATE '1901-01-01') = coalesce(stg.creation_date, DATE '1901-01-01')
         AND coalesce(tgt.creation_date_time , CAST('1901-01-01 00:00:00' AS datetime)) = coalesce(stg.creation_date_time , CAST('1901-01-01 00:00:00' AS datetime))
		 AND trim(CAST(coalesce(tgt.recruitment_source_id, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_source_id, -999) as STRING))
         AND trim(CAST(coalesce(tgt.recruitment_source_auto_filled_sw, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_source_auto_filled_sw, -999) as STRING))
         AND trim(CAST(coalesce(tgt.job_application_num, -999) as STRING)) = trim(CAST(coalesce(stg.job_application_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.requisition_num, -999) as STRING)) = trim(CAST(coalesce(stg.requisition_num, -999) as STRING))
         AND trim(CAST(coalesce(tgt.candidate_num, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_num, -999) as STRING))
         AND DATE(tgt.valid_to_date) ='9999-12-31'
         AND tgt.source_system_code = stg.source_system_code
      WHERE tgt.candidate_profile_sid IS NULL
  ;

    SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            Candidate_Profile_SID ,Valid_From_Date 
        from {{ params.param_hr_core_dataset_name }}.candidate_profile
        group by Candidate_Profile_SID ,Valid_From_Date 
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.candidate_profile');
    ELSE
      COMMIT TRANSACTION;
    END IF;
END;
