-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Candidate_Onboarding_Resource_WRK.sql
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
/*  Merge the data into EDWHR.REF_ONBOARDING_TOUR_STATUS table  */
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'ATS_RESOURCESCREENPACK_BCT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_resource_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_resource_wrk (resource_screening_package_num, valid_from_date, candidate_sid, recruitment_requisition_sid, valid_to_date, source_system_code, dw_last_update_date_time)
    SELECT
        stg.resourcescreeningpackage AS resource_screening_package_num,
        current_date() AS valid_from_date,
        can.candidate_sid,
        req.recruitment_requisition_sid,
        DATE '9999-12-31' AS valid_to_date,
        'B' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.ats_resourcescreenpack_bct_stg AS stg
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate AS can ON can.candidate_num = stg.candidate
         AND upper(can.valid_to_date) = '9999-12-31'
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.recruitment_requisition AS req ON req.requisition_num = stg.jobrequisition
         AND upper(req.valid_to_date) = '9999-12-31'
         AND upper(req.source_system_code) = 'B'
      WHERE upper(stg.screeningpackage) LIKE 'B%'
      GROUP BY 1, 2, 3, 4, 5, 6, 7
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Collect Statistics on the Target Table    */
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_RESOURCE_WRK');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
