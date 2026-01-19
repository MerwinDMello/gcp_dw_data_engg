-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Candidate_Onboarding_Core.sql
-- Translated from: bteq
-- Translated to: BigQuery

DECLARE _ERROR_CODE INT64;
DECLARE _ERROR_MSG STRING DEFAULT '';
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  -- No target-dialect support for source-dialect-specific SET
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/* Retire the records those are changed */
/* Begin Transaction Block Starts Here */
BEGIN
  SET _ERROR_CODE = 0;
  BEGIN TRANSACTION;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
BEGIN
  SET _ERROR_CODE = 0;
  UPDATE `hca-hin-dev-cur-hr`.`$ncr_tgt_schema`.candidate_onboarding AS tgt SET valid_to_date = DATE(current_date() - INTERVAL 1 DAY), dw_last_update_date_time = timestamp_trunc(current_timestamp(), SECOND) FROM `hca-hin-dev-cur-hr`.`$ncr_stg_schema`.candidate_onboarding_wrk AS wrk WHERE wrk.candidate_onboarding_sid = tgt.candidate_onboarding_sid
   AND (coalesce(trim(wrk.requisition_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.requisition_sid), CAST(123456 as STRING))
   OR coalesce(trim(wrk.employee_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.employee_sid), CAST(123456 as STRING))
   OR coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)) <> coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING))
   OR coalesce(trim(wrk.candidate_first_name), 'XXX') <> coalesce(trim(tgt.candidate_first_name), 'XXX')
   OR coalesce(trim(wrk.candidate_last_name), 'XXX') <> coalesce(trim(tgt.candidate_last_name), 'XXX')
   OR coalesce(trim(wrk.tour_start_date), '9999-12-30') <> coalesce(trim(tgt.tour_start_date), '9999-12-30')
   OR coalesce(trim(wrk.tour_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_id), CAST(123456 as STRING))
   OR coalesce(trim(wrk.tour_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_status_id), CAST(123456 as STRING))
   OR coalesce(trim(wrk.tour_completion_pct), CAST(123456 as STRING)) <> coalesce(trim(tgt.tour_completion_pct), CAST(123456 as STRING))
   OR coalesce(trim(wrk.workflow_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.workflow_id), CAST(123456 as STRING))
   OR coalesce(trim(wrk.workflow_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.workflow_status_id), CAST(123456 as STRING))
   OR coalesce(trim(wrk.email_sent_status_id), CAST(123456 as STRING)) <> coalesce(trim(tgt.email_sent_status_id), CAST(123456 as STRING))
   OR coalesce(trim(wrk.onboarding_confirmation_date), '9999-12-30') <> coalesce(trim(tgt.onboarding_confirmation_date), '9999-12-30')
   OR coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') <> coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
   OR coalesce(trim(wrk.process_level_code), 'XXX') <> coalesce(trim(tgt.process_level_code), 'XXX')
   OR coalesce(trim(wrk.applicant_num), CAST(123456 as STRING)) <> coalesce(trim(tgt.applicant_num), CAST(123456 as STRING))
   OR coalesce(trim(wrk.source_system_code), 'XXX') <> coalesce(trim(tgt.source_system_code), 'XXX'))
   AND upper(tgt.valid_to_date) = '9999-12-31';
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.`$ncr_tgt_schema`.candidate_onboarding (candidate_onboarding_sid, valid_from_date, valid_to_date, requisition_sid, employee_sid, candidate_sid, candidate_first_name, candidate_last_name, tour_start_date, tour_id, tour_status_id, tour_completion_pct, workflow_id, workflow_status_id, email_sent_status_id, onboarding_confirmation_date, recruitment_requisition_num_text, process_level_code, applicant_num, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.candidate_onboarding_sid,
        wrk.valid_from_date AS valid_from_date,
        '9999-12-31' AS valid_to_date,
        wrk.requisition_sid,
        wrk.employee_sid,
        wrk.candidate_sid,
        wrk.candidate_first_name,
        wrk.candidate_last_name,
        wrk.tour_start_date,
        wrk.tour_id,
        wrk.tour_status_id,
        wrk.tour_completion_pct,
        wrk.workflow_id,
        wrk.workflow_status_id,
        wrk.email_sent_status_id,
        wrk.onboarding_confirmation_date,
        wrk.recruitment_requisition_num_text,
        wrk.process_level_code,
        wrk.applicant_num,
        wrk.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.`$ncr_stg_schema`.candidate_onboarding_wrk AS wrk
      WHERE (coalesce(trim(wrk.candidate_onboarding_sid), CAST(123456 as STRING)), coalesce(trim(wrk.requisition_sid), CAST(123456 as STRING)), coalesce(trim(wrk.employee_sid), CAST(123456 as STRING)), coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)), coalesce(trim(wrk.candidate_first_name), 'XXX'), coalesce(trim(wrk.candidate_last_name), 'XXX'), coalesce(trim(wrk.tour_start_date), '9999-12-30'), coalesce(trim(wrk.tour_id), CAST(123456 as STRING)), coalesce(trim(wrk.tour_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.tour_completion_pct), CAST(123456 as STRING)), coalesce(trim(wrk.workflow_id), CAST(123456 as STRING)), coalesce(trim(wrk.workflow_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.email_sent_status_id), CAST(123456 as STRING)), coalesce(trim(wrk.onboarding_confirmation_date), '9999-12-30'), coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX'), coalesce(trim(wrk.process_level_code), 'XXX'), coalesce(trim(wrk.applicant_num), CAST(123456 as STRING)), coalesce(trim(wrk.source_system_code), 'XXX')) NOT IN(
        SELECT AS STRUCT
            coalesce(trim(tgt.candidate_onboarding_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.requisition_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.employee_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.candidate_first_name), 'XXX'),
            coalesce(trim(tgt.candidate_last_name), 'XXX'),
            coalesce(trim(tgt.tour_start_date), '9999-12-30'),
            coalesce(trim(tgt.tour_id), CAST(123456 as STRING)),
            coalesce(trim(tgt.tour_status_id), CAST(123456 as STRING)),
            coalesce(trim(tgt.tour_completion_pct), CAST(123456 as STRING)),
            coalesce(trim(tgt.workflow_id), CAST(123456 as STRING)),
            coalesce(trim(tgt.workflow_status_id), CAST(123456 as STRING)),
            coalesce(trim(tgt.email_sent_status_id), CAST(123456 as STRING)),
            coalesce(trim(tgt.onboarding_confirmation_date), '9999-12-30'),
            coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX'),
            coalesce(trim(tgt.process_level_code), 'XXX'),
            coalesce(trim(tgt.applicant_num), CAST(123456 as STRING)),
            coalesce(trim(tgt.source_system_code), 'XXX')
          FROM
            `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_onboarding AS tgt
          WHERE upper(tgt.valid_to_date) = '9999-12-31'
      )
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
/* End Transaction Block comment */
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('$NCR_TGT_SCHEMA', 'Candidate_Onboarding');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
