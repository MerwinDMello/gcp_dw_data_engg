-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Candidate_Onboarding_Resource.sql
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
/* BEGIN TRANSACTION BLOCK STARTS HERE */
BEGIN
  SET _ERROR_CODE = 0;
  BEGIN TRANSACTION;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
BEGIN
  SET _ERROR_CODE = 0;
  UPDATE `hca-hin-dev-cur-hr`.edwhr.candidate_onboarding_resource AS tgt SET valid_to_date = DATE(stg.valid_from_date - INTERVAL 1 DAY), dw_last_update_date_time = stg.dw_last_update_date_time FROM `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_resource_wrk AS stg WHERE tgt.resource_screening_package_num = stg.resource_screening_package_num
   AND (trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
   OR trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) <> trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
   OR coalesce(trim(tgt.source_system_code), 'X') <> coalesce(trim(stg.source_system_code), 'X'))
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
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr.candidate_onboarding_resource (resource_screening_package_num, valid_from_date, candidate_sid, recruitment_requisition_sid, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.resource_screening_package_num,
        stg.valid_from_date,
        stg.candidate_sid,
        stg.recruitment_requisition_sid,
        stg.valid_to_date,
        stg.source_system_code,
        stg.dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_resource_wrk AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_onboarding_resource AS tgt ON tgt.resource_screening_package_num = stg.resource_screening_package_num
         AND trim(CAST(coalesce(tgt.candidate_sid, -999) as STRING)) = trim(CAST(coalesce(stg.candidate_sid, -999) as STRING))
         AND trim(CAST(coalesce(tgt.recruitment_requisition_sid, -999) as STRING)) = trim(CAST(coalesce(stg.recruitment_requisition_sid, -999) as STRING))
         AND coalesce(trim(tgt.source_system_code), 'X') = coalesce(trim(stg.source_system_code), 'X')
         AND upper(tgt.valid_to_date) = '9999-12-31'
      WHERE tgt.resource_screening_package_num IS NULL
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
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  COLLECT STATISTICS ON THE TARGET TABLE    */
CALL dbadmin_procs.collect_stats_table('EDWHR', 'CANDIDATE_ONBOARDING_RESOURCE');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
