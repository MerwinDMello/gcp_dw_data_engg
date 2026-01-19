/*  Close the previous records from Target table for same key for any Changes  */
/*  Insert the New Records/Chnages into the Target Table  */
/* Begin Transaction Block Starts Here */

BEGIN
	  DECLARE dup_count INT64;
	  DECLARE current_dt DATETIME;
	  SET current_dt = DATETIME_TRUNC(CURRENT_DATETIME('US/Central'), SECOND);

	  BEGIN TRANSACTION;
  
  UPDATE {{ params.param_hr_core_dataset_name }}.submission AS tgt SET valid_to_date = DATETIME(current_dt - INTERVAL 1 SECOND), dw_last_update_date_time = stg.dw_last_update_date_time FROM {{ params.param_hr_stage_dataset_name }}.submission_wrk AS stg WHERE tgt.submission_sid = stg.submission_sid
   AND (coalesce(tgt.submission_num, -999) <> coalesce(stg.submission_num, -999)
   OR coalesce(tgt.last_modified_date, DATETIME '1901-01-01') <> coalesce(stg.last_modified_date, DATETIME '1901-01-01')
   OR coalesce(tgt.new_submission_sw, -999) <> coalesce(stg.new_submission_sw, -999)
   OR coalesce(tgt.candidate_sid, -999) <> coalesce(stg.candidate_sid, -999)
   OR coalesce(tgt.recruitment_requisition_sid, -999) <> coalesce(stg.recruitment_requisition_sid, -999)
   OR coalesce(tgt.candidate_profile_sid, -999) <> coalesce(stg.candidate_profile_sid, -999)
   OR coalesce(tgt.current_submission_status_id, -999) <> coalesce(stg.current_submission_status_id, -999)
   OR coalesce(tgt.current_submission_step_id, -999) <> coalesce(stg.current_submission_step_id, -999)
   OR coalesce(tgt.current_submission_workflow_id, -999) <> coalesce(stg.current_submission_workflow_id, -999)
   OR coalesce(tgt.requisition_num, -999) <> coalesce(stg.requisition_num, -999)
   OR coalesce(tgt.job_application_num, -999) <> coalesce(stg.job_application_num, -999)
   OR coalesce(tgt.candidate_num, -999) <> coalesce(stg.candidate_num, -999)
   OR coalesce(tgt.matched_from_requisition_num, -999) <> coalesce(stg.matched_from_requisition_num, -999)
   OR coalesce(trim(tgt.matched_candidate_flag), 'X') <> coalesce(trim(stg.matched_candidate_flag), 'X')
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X')
   OR coalesce(trim(tgt.submission_source_code), 'X') <> coalesce(trim(stg.submission_source_code), 'X'))
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");


  INSERT INTO {{ params.param_hr_core_dataset_name }}.submission (submission_sid, valid_from_date, valid_to_date, submission_num, last_modified_date, new_submission_sw, candidate_sid, recruitment_requisition_sid, candidate_profile_sid, current_submission_status_id, current_submission_step_id, current_submission_workflow_id, requisition_num, job_application_num, candidate_num, matched_from_requisition_num, matched_candidate_flag, submission_source_code, source_system_code, dw_last_update_date_time)
    SELECT
        stg.submission_sid,
        current_dt as valid_from_date,
        DATETIME("9999-12-31 23:59:59") as valid_to_date,
        stg.submission_num,
        stg.last_modified_date,
        stg.new_submission_sw,
        stg.candidate_sid,
        stg.recruitment_requisition_sid,
        stg.candidate_profile_sid,
        stg.current_submission_status_id,
        stg.current_submission_step_id,
        stg.current_submission_workflow_id,
        stg.requisition_num,
        stg.job_application_num,
        stg.candidate_num,
        stg.matched_from_requisition_num,
        stg.matched_candidate_flag,
        stg.submission_source_code,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.submission_wrk AS stg
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.submission AS tgt ON stg.submission_sid = tgt.submission_sid
         AND coalesce(tgt.submission_num, -999) = coalesce(stg.submission_num, -999)
         AND coalesce(tgt.last_modified_date, DATETIME '1901-01-01') = coalesce(stg.last_modified_date, DATE '1901-01-01')
         AND coalesce(tgt.new_submission_sw, -999) = coalesce(stg.new_submission_sw, -999)
         AND coalesce(tgt.candidate_sid, -999) = coalesce(stg.candidate_sid, -999)
         AND coalesce(tgt.recruitment_requisition_sid, -999) = coalesce(stg.recruitment_requisition_sid, -999)
         AND coalesce(tgt.candidate_profile_sid, -999) = coalesce(stg.candidate_profile_sid, -999)
         AND coalesce(tgt.current_submission_status_id, -999) = coalesce(stg.current_submission_status_id, -999)
         AND coalesce(tgt.current_submission_step_id, -999) = coalesce(stg.current_submission_step_id, -999)
         AND coalesce(tgt.requisition_num, -999) = coalesce(stg.requisition_num, -999)
         AND coalesce(tgt.job_application_num, -999) = coalesce(stg.job_application_num, -999)
         AND coalesce(tgt.candidate_num, -999) = coalesce(stg.candidate_num, -999)
         AND coalesce(tgt.matched_from_requisition_num, -999) = coalesce(stg.matched_from_requisition_num, -999)
         AND coalesce(trim(tgt.matched_candidate_flag), 'X') = coalesce(trim(stg.matched_candidate_flag), 'X')
         AND coalesce(tgt.current_submission_workflow_id, -999) = coalesce(stg.current_submission_workflow_id, -999)
         AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59")
         AND coalesce(trim(tgt.submission_source_code), 'X') = coalesce(trim(stg.submission_source_code), 'X')
		 WHERE tgt.submission_sid IS NULL ;


  UPDATE {{ params.param_hr_core_dataset_name }}.submission AS tgt SET matched_from_requisition_num = stg.requisition_number FROM {{ params.param_hr_stage_dataset_name }}.taleo_matched_requisition AS stg WHERE coalesce(tgt.submission_num, -999) = coalesce(stg.app_number, -999)
   AND upper(tgt.source_system_code) = 'T'
   AND tgt.valid_to_date = DATETIME("9999-12-31 23:59:59");

     SET DUP_COUNT = ( 
        select count(*)
        from (
        select
            submission_sid,valid_from_date
        from {{ params.param_hr_core_dataset_name }}.submission
        group by submission_sid,valid_from_date
        having count(*) > 1
        )
    );
    IF DUP_COUNT <> 0 THEN
      ROLLBACK TRANSACTION;
      RAISE USING MESSAGE = concat('Duplicates are not allowed in the table: {{ params.param_hr_core_dataset_name }}.submission');
    ELSE
      COMMIT TRANSACTION;
    END IF;

END;
