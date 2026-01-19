BEGIN
DECLARE
  DUP_COUNT INT64;

  UPDATE {{ params.param_hr_core_dataset_name }}.candidate_background_check AS tgt SET valid_to_date = DATETIME(current_datetime('US/Central') - INTERVAL 1 second), dw_last_update_date_time = timestamp_trunc(current_datetime('US/Central'), SECOND) FROM {{ params.param_hr_stage_dataset_name }}.candidate_background_check_wrk AS wrk WHERE wrk.report_sid = tgt.report_sid
   AND (coalesce(trim(CAST(wrk.report_create_date_time as STRING)), '1900-01-01') <> coalesce(trim(CAST(tgt.report_create_date_time as STRING)), '1900-01-01')
   OR coalesce(trim(wrk.candidate_first_name), 'XXX') <> coalesce(trim(tgt.candidate_first_name), 'XXX')
   OR coalesce(trim(wrk.candidate_middle_name), 'XXX') <> coalesce(trim(tgt.candidate_middle_name), 'XXX')
   OR coalesce(trim(wrk.candidate_last_name), 'XXX') <> coalesce(trim(tgt.candidate_last_name), 'XXX')
   OR coalesce(trim(wrk.social_security_num), 'XXX') <> coalesce(trim(tgt.social_security_num), 'XXX')
   OR coalesce(trim(CAST(wrk.report_reopen_date_time as STRING)), '1900-01-01') <> coalesce(trim(CAST(tgt.report_reopen_date_time as STRING)), '1900-01-01')
   OR coalesce(trim(CAST(wrk.report_completion_date_time as STRING)), '1900-01-01') <> coalesce(trim(CAST(tgt.report_completion_date_time as STRING)), '1900-01-01')
   OR coalesce(trim(wrk.process_level_code), 'XXX') <> coalesce(trim(tgt.process_level_code), 'XXX')
   OR coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') <> coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
   OR trim(CAST(coalesce(wrk.days_elapsed_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.days_elapsed_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.criminal_search_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.criminal_search_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.criminal_search_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.criminal_search_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.motor_vehicle_record_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.motor_vehicle_record_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.motor_vehicle_record_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.motor_vehicle_record_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.employment_verification_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.employment_verification_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.employment_verification_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.employment_verification_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.education_verification_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.education_verification_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.education_verification_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.education_verification_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.license_verification_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.license_verification_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.license_verification_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.license_verification_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.personal_reference_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.personal_reference_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.personal_reference_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.personal_reference_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.sanction_check_ordered_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.sanction_check_ordered_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.sanction_check_pending_cnt, 9) as STRING)) <> trim(CAST(coalesce(tgt.sanction_check_pending_cnt, 9) as STRING))
   OR trim(CAST(coalesce(wrk.report_num, 9) as STRING)) <> trim(CAST(coalesce(tgt.report_num, 9) as STRING))
   OR coalesce(trim(wrk.source_system_code), 'XXX') <> coalesce(trim(tgt.source_system_code), 'XXX'))
   AND DATE(tgt.valid_to_date) = DATE('9999-12-31');


  BEGIN TRANSACTION;
  INSERT INTO {{ params.param_hr_core_dataset_name }}.candidate_background_check (report_sid, valid_from_date, report_create_date_time, candidate_first_name, candidate_middle_name, candidate_last_name, social_security_num, report_reopen_date_time, report_completion_date_time, process_level_code, recruitment_requisition_num_text, days_elapsed_cnt, criminal_search_ordered_cnt, criminal_search_pending_cnt, motor_vehicle_record_ordered_cnt, motor_vehicle_record_pending_cnt, employment_verification_ordered_cnt, employment_verification_pending_cnt, education_verification_ordered_cnt, education_verification_pending_cnt, license_verification_ordered_cnt, license_verification_pending_cnt, personal_reference_ordered_cnt, personal_reference_pending_cnt, sanction_check_ordered_cnt, sanction_check_pending_cnt, report_num, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.report_sid,
        wrk.valid_from_date,
        wrk.report_create_date_time,
        wrk.candidate_first_name,
        wrk.candidate_middle_name,
        wrk.candidate_last_name,
        wrk.social_security_num,
        wrk.report_reopen_date_time,
        wrk.report_completion_date_time,
        wrk.process_level_code,
        wrk.recruitment_requisition_num_text,
        wrk.days_elapsed_cnt,
        wrk.criminal_search_ordered_cnt,
        wrk.criminal_search_pending_cnt,
        wrk.motor_vehicle_record_ordered_cnt,
        wrk.motor_vehicle_record_pending_cnt,
        wrk.employment_verification_ordered_cnt,
        wrk.employment_verification_pending_cnt,
        wrk.education_verification_ordered_cnt,
        wrk.education_verification_pending_cnt,
        wrk.license_verification_ordered_cnt,
        wrk.license_verification_pending_cnt,
        wrk.personal_reference_ordered_cnt,
        wrk.personal_reference_pending_cnt,
        wrk.sanction_check_ordered_cnt,
        wrk.sanction_check_pending_cnt,
        wrk.report_num,
        wrk.valid_to_date,
        wrk.source_system_code,
        wrk.dw_last_update_date_time
      FROM
        {{ params.param_hr_stage_dataset_name }}.candidate_background_check_wrk AS wrk
        LEFT OUTER JOIN {{ params.param_hr_base_views_dataset_name }}.candidate_background_check AS tgt ON wrk.report_sid = tgt.report_sid
         AND coalesce(trim(CAST(wrk.report_create_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_create_date_time as STRING)), '1900-01-01')
         AND coalesce(trim(wrk.candidate_first_name), 'XXX') = coalesce(trim(tgt.candidate_first_name), 'XXX')
         AND coalesce(trim(wrk.candidate_middle_name), 'XXX') = coalesce(trim(tgt.candidate_middle_name), 'XXX')
         AND coalesce(trim(wrk.candidate_last_name), 'XXX') = coalesce(trim(tgt.candidate_last_name), 'XXX')
         AND coalesce(trim(wrk.social_security_num), 'XXX') = coalesce(trim(tgt.social_security_num), 'XXX')
         AND coalesce(trim(CAST(wrk.report_reopen_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_reopen_date_time as STRING)), '1900-01-01')
         AND coalesce(trim(CAST(wrk.report_completion_date_time as STRING)), '1900-01-01') = coalesce(trim(CAST(tgt.report_completion_date_time as STRING)), '1900-01-01')
         AND coalesce(trim(wrk.process_level_code), 'XXX') = coalesce(trim(tgt.process_level_code), 'XXX')
         AND coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') = coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
         AND trim(CAST(coalesce(wrk.days_elapsed_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.days_elapsed_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.criminal_search_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.criminal_search_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.criminal_search_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.criminal_search_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.motor_vehicle_record_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.motor_vehicle_record_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.motor_vehicle_record_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.motor_vehicle_record_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.employment_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.employment_verification_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.employment_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.employment_verification_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.education_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.education_verification_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.education_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.education_verification_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.license_verification_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.license_verification_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.license_verification_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.license_verification_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.personal_reference_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.personal_reference_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.personal_reference_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.personal_reference_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.sanction_check_ordered_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.sanction_check_ordered_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.sanction_check_pending_cnt, 9) as STRING)) = trim(CAST(coalesce(tgt.sanction_check_pending_cnt, 9) as STRING))
         AND trim(CAST(coalesce(wrk.report_num, 9) as STRING)) = trim(CAST(coalesce(tgt.report_num, 9) as STRING))
         AND coalesce(trim(wrk.source_system_code), 'XXX') = coalesce(trim(tgt.source_system_code), 'XXX')
         AND DATE(tgt.valid_to_date) = DATE('9999-12-31')
      WHERE tgt.report_sid IS NULL
  ;

        SET DUP_COUNT = ( 
        select count(*)
        from (
        select
           report_sid, valid_from_date
        from {{ params.param_hr_core_dataset_name }}.candidate_background_check
        group by report_sid, valid_from_date
        having count(*) > 1
        )
    );
        IF
         DUP_COUNT <> 0 THEN
          ROLLBACK TRANSACTION; RAISE
            USING
            MESSAGE = CONCAT('Duplicates are not allowed in the table: candidate_background_check');
        ELSE
        COMMIT TRANSACTION;
        END IF
  ;
END;