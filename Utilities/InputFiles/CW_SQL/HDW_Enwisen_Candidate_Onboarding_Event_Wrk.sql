-- Translation time: 2023-06-29T16:27:20.353096Z
-- Translation job ID: 6e127c9a-17a5-4754-a4c2-7e233bc0de27
-- Source: eim-cs-da-gmek-5764-dev/HRG/Bteq_Source_Files/MERWIN/Enwisen/HDW_Enwisen_Candidate_Onboarding_Event_Wrk.sql
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
/*  Generate the surrogate keys for Candidate_Onboarding_Event */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_wrk (requisition_number, process_level_code, data_type, dw_last_update_date_time)
    SELECT
        iq.requisition_number,
        iq.process_level_code,
        iq.data_type,
        iq.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), 1, strpos(reverse(enwisen_cm5tickets_stg.subject), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
              'T' AS data_type,
              timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
            WHERE upper(enwisen_cm5tickets_stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND upper(enwisen_cm5tickets_stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND upper(enwisen_cm5tickets_stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND strpos(enwisen_cm5tickets_stg.subject, '-') <> 0
             AND upper(enwisen_cm5tickets_stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
          UNION DISTINCT
          SELECT DISTINCT
              trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), 1, strpos(reverse(enwisen_cm5tickets_stg.subject), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
              'D' AS data_type,
              timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
            WHERE upper(enwisen_cm5tickets_stg.topic) = 'HIRE MY EMPLOYEES'
             AND upper(enwisen_cm5tickets_stg.category) = 'HIRING EMPLOYEES'
             AND upper(enwisen_cm5tickets_stg.subcategory) = 'DRUG SCREEN RESULTS'
             AND upper(enwisen_cm5tickets_stg.subject) LIKE 'CONFIRMED DS RESULTS%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
          UNION DISTINCT
          SELECT DISTINCT
              trim(reverse(trim(substr(replace(reverse(enwisen_cm5tickets_stg.subject), ')', ''), 1, strpos(replace(reverse(enwisen_cm5tickets_stg.subject), ')', ''), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(enwisen_cm5tickets_stg.subject), strpos(reverse(enwisen_cm5tickets_stg.subject), '-') + 1, 5)))) AS process_level_code,
              'B' AS data_type,
              timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg
            WHERE upper(enwisen_cm5tickets_stg.topic) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(enwisen_cm5tickets_stg.category) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(enwisen_cm5tickets_stg.subcategory) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(enwisen_cm5tickets_stg.subject) LIKE 'BG AUTH COMPLETE%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ -%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ-%'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQ %'
             AND upper(enwisen_cm5tickets_stg.subject) NOT LIKE '%ACQUISITION%'
             AND strpos(enwisen_cm5tickets_stg.subject, '-') <> 0
        ) AS iq
      WHERE length(iq.requisition_number) <> 0
       AND length(iq.process_level_code) <> 0
       AND lower(iq.requisition_number) = upper(iq.requisition_number)
       AND lower(iq.process_level_code) = upper(iq.process_level_code)
       AND iq.requisition_number NOT LIKE '%*%'
       AND iq.requisition_number NOT LIKE '%/%'
       AND iq.requisition_number NOT LIKE '%)%'
       AND iq.requisition_number NOT LIKE '%(%'
       AND iq.requisition_number NOT LIKE '%.%'
       AND iq.requisition_number NOT LIKE '%]%'
       AND iq.requisition_number NOT LIKE '%[%'
       AND iq.requisition_number NOT LIKE '% %'
       AND iq.requisition_number NOT LIKE '%:%'
       AND iq.requisition_number NOT LIKE '%\'%'
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_WRK');
CALL edwhr_procs.sk_gen('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_WRK', 'Coalesce(Trim(Requisition_Number)||Trim(Process_Level_Code)||Trim(Data_Type),-1)', 'CANDIDATE_ONBOARDING_EVENT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Truncate Work Table     */
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_wrk;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
/*  Load Work Table with working Data */
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_wrk (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        roe.event_type_id AS event_type_id,
        concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1))))) AS recruitment_requisition_num_text,
        CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
        CAST(NULL as INT64) AS candidate_sid,
        CAST(NULL as INT64) AS resource_screening_package_num,
        CAST(NULL as INT64) AS sequence_num,
        'W' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(reverse(subject), 1, strpos(reverse(subject), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'T'), CAST(-1 as STRING)) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'T'
      WHERE upper(topic) = 'Z-AUTO-ONBOARDING CONFIRM'
       AND upper(category) = 'Z-AUTO-ONBOARDING CONFIRM'
       AND upper(subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
       AND strpos(subject, '-') <> 0
       AND upper(subject) LIKE 'ONBOARDING ACTION REQUIRED %'
       AND upper(subject) NOT LIKE '%ACQ -%'
       AND upper(subject) NOT LIKE '%ACQ-%'
       AND upper(subject) NOT LIKE '%ACQ %'
       AND upper(subject) NOT LIKE '%ACQUISITION%'
      QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
    UNION DISTINCT
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        roe.event_type_id AS event_type_id,
        concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1))))) AS recruitment_requisition_num_text,
        CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
        CAST(NULL as INT64) AS candidate_sid,
        CAST(NULL as INT64) AS resource_screening_package_num,
        CAST(NULL as INT64) AS sequence_num,
        'W' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(reverse(subject), 1, strpos(reverse(subject), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'D'), CAST(-1 as STRING)) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'D'
      WHERE upper(topic) = 'HIRE MY EMPLOYEES'
       AND upper(category) = 'HIRING EMPLOYEES'
       AND upper(subcategory) = 'DRUG SCREEN RESULTS'
       AND upper(subject) LIKE 'CONFIRMED DS RESULTS%'
       AND upper(subject) NOT LIKE '%ACQ -%'
       AND upper(subject) NOT LIKE '%ACQ-%'
       AND upper(subject) NOT LIKE '%ACQ %'
       AND upper(subject) NOT LIKE '%ACQUISITION%'
      QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
    UNION DISTINCT
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        roe.event_type_id AS event_type_id,
        concat(trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))), '-', trim(reverse(trim(substr(replace(reverse(subject), ')', ''), 1, strpos(replace(reverse(subject), ')', ''), '-') - 1))))) AS recruitment_requisition_num_text,
        CAST(trim(stg.createddatetime) as TIMESTAMP) AS completed_date,
        CAST(NULL as INT64) AS candidate_sid,
        CAST(NULL as INT64) AS resource_screening_package_num,
        CAST(NULL as INT64) AS sequence_num,
        'W' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON coalesce(concat(trim(reverse(trim(substr(replace(reverse(subject), ')', ''), 1, strpos(replace(reverse(subject), ')', ''), '-') - 1)))), trim(reverse(trim(substr(reverse(subject), strpos(reverse(subject), '-') + 1, 5)))), 'B'), CAST(-1 as STRING)) = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_onboarding_event_type AS roe ON upper(event_type_code) = 'B'
      WHERE upper(topic) = 'Z-AUTO-BG CHECK AUTH'
       AND upper(category) = 'Z-AUTO-BG CHECK AUTH'
       AND upper(subcategory) = 'Z-AUTO-BG CHECK AUTH'
       AND upper(subject) LIKE 'BG AUTH COMPLETE%'
       AND upper(subject) NOT LIKE '%ACQ -%'
       AND upper(subject) NOT LIKE '%ACQ-%'
       AND upper(subject) NOT LIKE '%ACQ %'
       AND upper(subject) NOT LIKE '%ACQUISITION%'
       AND strpos(subject, '-') <> 0
      QUALIFY row_number() OVER (PARTITION BY candidate_onboarding_event_sid ORDER BY stg.createddatetime DESC) = 1
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
-- -HDM-1921
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'ATS_RESOURCETRANSITION_BCT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'ATS_RESOURCESCREENING_BCT_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_ats;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_ats (jobrequisition, candidate, event_type_id, dw_last_update_date_time)
    SELECT
        max(trim(ats_resourcetransition_bct_stg.jobrequisition)) AS jobrequisition,
        max(trim(ats_resourcetransition_bct_stg.candidate)) AS candidate,
        '2' AS event_type_id,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.ats_resourcetransition_bct_stg
      WHERE upper(ats_resourcetransition_bct_stg.status_state) = 'COMPLETE'
      GROUP BY upper(trim(ats_resourcetransition_bct_stg.jobrequisition)), upper(trim(ats_resourcetransition_bct_stg.candidate)), 4
    UNION DISTINCT
    SELECT
        max(trim(ats_resourcescreening_bct_stg.resourcescreeningpackage)) AS jobrequisition,
        max(trim(ats_resourcescreening_bct_stg.sequencenumber)) AS candidate,
        '1' AS event_type_id,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.ats_resourcescreening_bct_stg
      WHERE upper(ats_resourcescreening_bct_stg.hcascrevendstat) = 'COMPLETED'
       AND upper(ats_resourcescreening_bct_stg.screening) = 'DRUGSCREEN'
      GROUP BY upper(trim(ats_resourcescreening_bct_stg.resourcescreeningpackage)), upper(trim(ats_resourcescreening_bct_stg.sequencenumber)), 4
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
/*UNION
SELECT 
TRIM(CAST (RESOURCESCREENINGPACKAGE AS VARCHAR(20))) AS JOBREQUISITION,
TRIM(CAST(SEQUENCENUMBER AS VARCHAR(20))) AS CANDIDATE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG 
WHERE HCASCREVENDSTAT = 'HRREVIEWRECRUITER'
GROUP BY 1,2,3;*/
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_ATS');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL edwhr_procs.sk_gen('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_ATS', 'TRIM(JOBREQUISITION)||TRIM(CANDIDATE)||EVENT_TYPE_ID||\'-B\'', 'CANDIDATE_ONBOARDING_EVENT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  TRUNCATE TABLE `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_stg;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_stg (requisition_num, candidate_num, event_type_id, recruitment_requisition_num_text, creation_date_time, candidate_sid, dw_last_update_date_time)
    SELECT
        max(trim(s.requisition_num)) AS requisition_num,
        max(trim(s.candidate_num)) AS candidate_num,
        '3' AS event_type_id,
        rr.recruitment_requisition_num_text,
        st.creation_date_time,
        s.candidate_sid,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_base_views.submission_tracking AS st
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.submission_tracking_status AS sts ON st.submission_tracking_sid = sts.submission_tracking_sid
         AND upper(sts.valid_to_date) = '9999-12-31'
         AND upper(sts.source_system_code) = 'B'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.ref_submission_status AS rss ON sts.submission_status_id = rss.submission_status_id
         AND upper(rss.source_system_code) = 'B'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_profile AS cp ON st.candidate_profile_sid = cp.candidate_profile_sid
         AND upper(cp.valid_to_date) = '9999-12-31'
         AND upper(cp.source_system_code) = 'B'
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.submission AS s ON cp.candidate_profile_sid = s.candidate_profile_sid
         AND upper(s.valid_to_date) = '9999-12-31'
         AND upper(s.source_system_code) = 'B'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.recruitment_requisition AS rr ON s.recruitment_requisition_sid = rr.recruitment_requisition_sid
         AND upper(rr.valid_to_date) = '9999-12-31'
         AND upper(rr.source_system_code) = 'B'
      WHERE upper(st.valid_to_date) = '9999-12-31'
       AND upper(st.source_system_code) = 'B'
       AND upper(rss.submission_status_desc) = 'CANDIDATE CONSENT PENDING'
      GROUP BY upper(trim(s.requisition_num)), upper(trim(s.candidate_num)), 4, 5, 6, 7
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_STG');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL edwhr_procs.sk_gen('EDWHR_STAGING', 'CANDIDATE_ONBOARDING_EVENT_XWLK_STG', 'REQUISITION_NUM||CANDIDATE_NUM||EVENT_TYPE_ID||\'-B\'', 'CANDIDATE_ONBOARDING_EVENT');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_wrk (candidate_onboarding_event_sid, valid_from_date, event_type_id, recruitment_requisition_num_text, completed_date, candidate_sid, resource_screening_package_num, sequence_num, source_system_code, dw_last_update_date_time)
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        '2' AS event_type_id,
        req.recruitment_requisition_num_text AS recruitment_requisition_num_text,
        stg.completion_date AS completed_date,
        can.candidate_sid AS candidate_sid,
        orc.resource_screening_package_num AS resource_screening_package_num,
        CAST(NULL as INT64) AS sequence_num,
        'B' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.ats_resourcetransition_bct_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON concat(trim(stg.jobrequisition), trim(stg.candidate), '2', '-B') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.recruitment_requisition AS req ON req.requisition_num = stg.jobrequisition
         AND upper(req.valid_to_date) = '9999-12-31'
         AND upper(req.source_system_code) = 'B'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate AS can ON can.candidate_num = stg.candidate
         AND upper(can.valid_to_date) = '9999-12-31'
         AND upper(can.source_system_code) = 'B'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_onboarding_resource AS orc ON orc.candidate_sid = can.candidate_sid
         AND upper(orc.valid_to_date) = '9999-12-31'
         AND upper(orc.source_system_code) = 'B'
      WHERE upper(stg.status_state) = 'COMPLETE'
      QUALIFY row_number() OVER (PARTITION BY stg.jobrequisition, stg.candidate ORDER BY stg.updatestamp DESC) = 1
    UNION DISTINCT
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        '1' AS event_type_id,
        req.recruitment_requisition_num_text AS recruitment_requisition_num_text,
        stg.hcascrevendstatdate AS completed_date,
        orc.candidate_sid AS candidate_sid,
        stg.resourcescreeningpackage AS resource_screening_package_num,
        stg.sequencenumber AS sequence_num,
        'B' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.ats_resourcescreening_bct_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON upper(concat(trim(stg.resourcescreeningpackage), trim(stg.sequencenumber), '1', '-B')) = upper(xwlk.sk_source_txt)
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.candidate_onboarding_resource AS orc ON orc.resource_screening_package_num = stg.resourcescreeningpackage
         AND upper(orc.valid_to_date) = '9999-12-31'
         AND upper(orc.source_system_code) = 'B'
        LEFT OUTER JOIN `hca-hin-dev-cur-hr`.edwhr_base_views.recruitment_requisition AS req ON req.recruitment_requisition_sid = orc.recruitment_requisition_sid
         AND upper(req.valid_to_date) = '9999-12-31'
         AND upper(req.source_system_code) = 'B'
      WHERE upper(stg.hcascrevendstat) = 'COMPLETED'
       AND upper(stg.screening) = 'DRUGSCREEN'
      QUALIFY row_number() OVER (PARTITION BY resource_screening_package_num, stg.sequencenumber ORDER BY stg.updatestamp DESC) = 1
    UNION DISTINCT
    SELECT
        xwlk.sk AS candidate_onboarding_event_sid,
        current_date() AS valid_from_date,
        stg.event_type_id AS event_type_id,
        stg.recruitment_requisition_num_text AS recruitment_requisition_num_text,
        stg.creation_date_time AS completed_date,
        stg.candidate_sid AS candidate_sid,
        NULL AS resource_screening_package_num,
        CAST(NULL as INT64) AS sequence_num,
        'B' AS source_system_code,
        timestamp_trunc(current_timestamp(), SECOND) AS dw_last_update_date_time
      FROM
        `hca-hin-dev-cur-hr`.edwhr_staging.candidate_onboarding_event_xwlk_stg AS stg
        INNER JOIN `hca-hin-dev-cur-hr`.edwhr_staging.ref_sk_xwlk AS xwlk ON concat(stg.requisition_num, stg.candidate_num, stg.event_type_id, '-B') = xwlk.sk_source_txt
         AND upper(xwlk.sk_type) = 'CANDIDATE_ONBOARDING_EVENT'
      QUALIFY row_number() OVER (PARTITION BY stg.requisition_num, stg.candidate_num ORDER BY stg.creation_date_time DESC) = 1
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
-- GROUP BY 1,2,3,4,5,6,7,8,9,10;
/*SELECT
XWLK.SK AS CANDIDATE_ONBOARDING_EVENT_SID,
CURRENT_DATE AS VALID_FROM_DATE,
CAST('3' AS CHAR(1))AS EVENT_TYPE_ID,
REQ.RECRUITMENT_REQUISITION_NUM_TEXT AS RECRUITMENT_REQUISITION_NUM_TEXT,
CAST(STG.HCASCREVENDSTATDATE AS TIMESTAMP(0))AS COMPLETED_DATE,
ORC.CANDIDATE_SID AS CANDIDATE_SID,
STG.RESOURCESCREENINGPACKAGE AS RESOURCE_SCREENING_PACKAGE_NUM,
STG.SEQUENCENUMBER  AS SEQUENCE_NUM,
'B' AS SOURCE_SYSTEM_CODE,
CURRENT_TIMESTAMP(0) AS DW_LAST_UPDATE_DATE_TIME
FROM EDWHR_STAGING.ATS_RESOURCESCREENING_BCT_STG STG
INNER JOIN EDWHR_STAGING.REF_SK_XWLK XWLK
ON  TRIM(CAST(STG.RESOURCESCREENINGPACKAGE AS VARCHAR(20))) ||TRIM(CAST(STG.SEQUENCENUMBER AS VARCHAR(20)))||'-B'  = XWLK.SK_SOURCE_TXT   
AND XWLK.SK_TYPE = 'CANDIDATE_ONBOARDING_EVENT'
LEFT JOIN EDWHR_BASE_VIEWS.CANDIDATE_ONBOARDING_RESOURCE ORC
ON ORC.RESOURCE_SCREENING_PACKAGE_NUM=STG.RESOURCESCREENINGPACKAGE
AND  ORC.VALID_TO_DATE = '9999-12-31' 
AND ORC.SOURCE_SYSTEM_CODE = 'B'
LEFT JOIN EDWHR_BASE_VIEWS.RECRUITMENT_REQUISITION REQ
ON REQ.RECRUITMENT_REQUISITION_SID=ORC.RECRUITMENT_REQUISITION_SID
AND  REQ.VALID_TO_DATE = '9999-12-31' 
AND REQ.SOURCE_SYSTEM_CODE = 'B'
WHERE STG.HCASCREVENDSTAT = 'HRREVIEWRECRUITER'
GROUP BY 1,2,3,4,5,6,7,8,9,10;*/
-- Inserting Rejected Records which are filtered above
BEGIN
  SET _ERROR_CODE = 0;
  INSERT INTO `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg_reject (ticketcode, userid, archived, casetype, ticketstatus, createddatetime, sladate, closeddatetime, lasteditdatetime, resolveddatetime, topic, category, subcategory, source_code, servicegroup, substatus, isfirstcallresolution, creatoruserid, closeuserid, lastedituserid, owneruserid, chatagentuserid, regardinguserid, creatorfirstname, creatorlastname, creatorname, contactmethod, contactname, contactrelationshipname, aboutee, email, firstname, lastname, surveydatetime, surveyid, surveyanswer1, surveyanswer2, surveyanswer3, surveyanswer4, surveyanswer5, surveyscore, surveyagreementresponse, population, priority, processtime, reminderdatetime, reminderemail, reminderphone, secure, showtoee, customcheckbox1, customcheckbox2, customcheckbox3, customcheckbox4, customcheckbox5, customcheckbox6, customdate1, customdate2, customdate3, customdate4, customdate5, customdate6, customselect1, customselect2, customselect3, customselect4, customselect5, customselect6, customstring1, customstring2, customstring3, customstring4, customstring5, customstring6, surveyfollowup, knowledgedomain, remindernote, surveycommentresponse, subject, resolution, issue, dw_last_update_date_time)
    SELECT DISTINCT
        iq.ticketcode,
        iq.userid,
        iq.archived,
        iq.casetype,
        iq.ticketstatus,
        iq.createddatetime,
        iq.sladate,
        iq.closeddatetime,
        iq.lasteditdatetime,
        iq.resolveddatetime,
        iq.topic,
        iq.category,
        iq.subcategory,
        iq.source_code,
        iq.servicegroup,
        iq.substatus,
        iq.isfirstcallresolution,
        iq.creatoruserid,
        iq.closeuserid,
        iq.lastedituserid,
        iq.owneruserid,
        iq.chatagentuserid,
        iq.regardinguserid,
        iq.creatorfirstname,
        iq.creatorlastname,
        iq.creatorname,
        iq.contactmethod,
        iq.contactname,
        iq.contactrelationshipname,
        iq.aboutee,
        iq.email,
        iq.firstname,
        iq.lastname,
        iq.surveydatetime,
        iq.surveyid,
        iq.surveyanswer1,
        iq.surveyanswer2,
        iq.surveyanswer3,
        iq.surveyanswer4,
        iq.surveyanswer5,
        iq.surveyscore,
        iq.surveyagreementresponse,
        iq.population,
        iq.priority,
        iq.processtime,
        iq.reminderdatetime,
        iq.reminderemail,
        iq.reminderphone,
        iq.secure,
        iq.showtoee,
        iq.customcheckbox1,
        iq.customcheckbox2,
        iq.customcheckbox3,
        iq.customcheckbox4,
        iq.customcheckbox5,
        iq.customcheckbox6,
        iq.customdate1,
        iq.customdate2,
        iq.customdate3,
        iq.customdate4,
        iq.customdate5,
        iq.customdate6,
        iq.customselect1,
        iq.customselect2,
        iq.customselect3,
        iq.customselect4,
        iq.customselect5,
        iq.customselect6,
        iq.customstring1,
        iq.customstring2,
        iq.customstring3,
        iq.customstring4,
        iq.customstring5,
        iq.customstring6,
        iq.surveyfollowup,
        iq.knowledgedomain,
        iq.remindernote,
        iq.surveycommentresponse,
        iq.subject,
        iq.resolution,
        iq.issue,
        iq.dw_last_update_date_time
      FROM
        (
          SELECT DISTINCT
              trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
              stg.*
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
            WHERE upper(stg.topic) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND upper(stg.category) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND upper(stg.subcategory) = 'Z-AUTO-ONBOARDING CONFIRM'
             AND strpos(stg.subject, '-') <> 0
             AND upper(stg.subject) LIKE 'ONBOARDING ACTION REQUIRED %'
             AND upper(stg.subject) NOT LIKE '%ACQ -%'
             AND upper(stg.subject) NOT LIKE '%ACQ-%'
             AND upper(stg.subject) NOT LIKE '%ACQ %'
             AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
          UNION DISTINCT
          SELECT DISTINCT
              trim(reverse(trim(substr(reverse(stg.subject), 1, strpos(reverse(stg.subject), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
              stg.*
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
            WHERE upper(stg.topic) = 'HIRE MY EMPLOYEES'
             AND upper(stg.category) = 'HIRING EMPLOYEES'
             AND upper(stg.subcategory) = 'DRUG SCREEN RESULTS'
             AND upper(stg.subject) LIKE 'CONFIRMED DS RESULTS%'
             AND upper(stg.subject) NOT LIKE '%ACQ -%'
             AND upper(stg.subject) NOT LIKE '%ACQ-%'
             AND upper(stg.subject) NOT LIKE '%ACQ %'
             AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
          UNION DISTINCT
          SELECT DISTINCT
              trim(reverse(trim(substr(replace(reverse(stg.subject), ')', ''), 1, strpos(replace(reverse(stg.subject), ')', ''), '-') - 1)))) AS requisition_number,
              trim(reverse(trim(substr(reverse(stg.subject), strpos(reverse(stg.subject), '-') + 1, 5)))) AS process_level_code,
              stg.*
            FROM
              `hca-hin-dev-cur-hr`.edwhr_staging.enwisen_cm5tickets_stg AS stg
            WHERE upper(stg.topic) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(stg.category) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(stg.subcategory) = 'Z-AUTO-BG CHECK AUTH'
             AND upper(stg.subject) LIKE 'BG AUTH COMPLETE%'
             AND upper(stg.subject) NOT LIKE '%ACQ -%'
             AND upper(stg.subject) NOT LIKE '%ACQ-%'
             AND upper(stg.subject) NOT LIKE '%ACQ %'
             AND upper(stg.subject) NOT LIKE '%ACQUISITION%'
             AND strpos(stg.subject, '-') <> 0
        ) AS iq
      WHERE length(iq.requisition_number) = 0
       OR length(iq.process_level_code) = 0
       OR lower(iq.requisition_number) <> upper(iq.requisition_number)
       OR lower(iq.process_level_code) <> upper(iq.process_level_code)
       OR iq.requisition_number LIKE '%*%'
       OR iq.requisition_number LIKE '%/%'
       OR iq.requisition_number LIKE '%)%'
       OR iq.requisition_number LIKE '%(%'
       OR iq.requisition_number LIKE '%.%'
       OR iq.requisition_number LIKE '%]%'
       OR iq.requisition_number LIKE '%[%'
       OR iq.requisition_number LIKE '% %'
       OR iq.requisition_number LIKE '%:%'
       OR iq.requisition_number LIKE '%\'%'
  ;
EXCEPTION WHEN ERROR THEN
  SET _ERROR_CODE = 1;
  SET _ERROR_MSG = @@error.message;
END;
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'Candidate_Onboarding_Event_Wrk');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
CALL dbadmin_procs.collect_stats_table('EDWHR_STAGING', 'Enwisen_CM5Tickets_Stg_Reject');
IF _ERROR_CODE <> 0 THEN
  RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);
END IF;
RETURN;
