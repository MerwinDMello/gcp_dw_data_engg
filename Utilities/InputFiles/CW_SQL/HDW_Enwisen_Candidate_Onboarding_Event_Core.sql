-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Candidate_Onboarding_Event_Core.sql
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
  UPDATE `hca-hin-dev-cur-hr`.`$ncr_tgt_schema`.candidate_onboarding_event AS tgt SET valid_to_date = DATE(current_date() - INTERVAL 1 DAY), dw_last_update_date_time = timestamp_trunc(current_timestamp(), SECOND) FROM `hca-hin-dev-cur-hr`.`$ncr_stg_schema`.candidate_onboarding_event_wrk AS wrk WHERE wrk.candidate_onboarding_event_sid = tgt.candidate_onboarding_event_sid
   AND (coalesce(trim(wrk.event_type_id), 'XXX') <> coalesce(trim(tgt.event_type_id), 'XXX')
   OR coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX') <> coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX')
   OR upper(coalesce(wrk.completed_date, '9999-12-30')) <> upper(coalesce(tgt.completed_date, '9999-12-30'))
   OR coalesce(trim(wrk.candidate_sid), 'XXX') <> coalesce(trim(tgt.candidate_sid), 'XXX')
   OR coalesce(trim(wrk.resource_screening_package_num), 'XXX') <> coalesce(trim(tgt.resource_screening_package_num), 'XXX')
   OR coalesce(trim(wrk.sequence_num), 'XXX') <> coalesce(trim(tgt.sequence_num), 'XXX')
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
  INSERT INTO `hca-hin-dev-cur-hr`.`$ncr_tgt_schema`.candidate_onboarding_event (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, valid_to_date, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
    SELECT
        wrk.candidate_onboarding_event_sid,
        wrk.valid_from_date AS valid_from_date,
        wrk.event_type_id,
        wrk.recruitment_requisition_num_text,
        '9999-12-31' AS valid_to_date,
        wrk.completed_date,
        wrk.candidate_sid,
        wrk.resource_screening_package_num,
        wrk.sequence_num,
        wrk.source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.`$ncr_stg_schema`.candidate_onboarding_event_wrk AS wrk
      WHERE (coalesce(trim(wrk.candidate_onboarding_event_sid), CAST(123456 as STRING)), coalesce(trim(wrk.event_type_id), 'XXX'), coalesce(trim(wrk.recruitment_requisition_num_text), 'XXX'), upper(coalesce(wrk.completed_date, '9999-12-30')), coalesce(trim(wrk.candidate_sid), CAST(123456 as STRING)), coalesce(trim(wrk.resource_screening_package_num), CAST(123456 as STRING)), coalesce(trim(wrk.sequence_num), CAST(123456 as STRING)), coalesce(trim(wrk.source_system_code), 'XXX')) NOT IN(
        SELECT AS STRUCT
            coalesce(trim(tgt.candidate_onboarding_event_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.event_type_id), 'XXX'),
            coalesce(trim(tgt.recruitment_requisition_num_text), 'XXX'),
            upper(coalesce(tgt.completed_date, '9999-12-30')),
            coalesce(trim(tgt.candidate_sid), CAST(123456 as STRING)),
            coalesce(trim(tgt.resource_screening_package_num), CAST(123456 as STRING)),
            coalesce(trim(tgt.sequence_num), CAST(123456 as STRING)),
            coalesce(trim(tgt.source_system_code), 'XXX')
          FROM
            `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_onboarding_event AS tgt
          WHERE upper(tgt.valid_to_date) = '9999-12-31'
      )
      QUALIFY row_number() OVER (PARTITION BY wrk.candidate_onboarding_event_sid, valid_from_date ORDER BY wrk.candidate_onboarding_event_sid, valid_from_date DESC) = 1
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
CALL dbadmin_procs.collect_stats_table('$NCR_TGT_SCHEMA', 'Candidate_Onboarding_Event');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
