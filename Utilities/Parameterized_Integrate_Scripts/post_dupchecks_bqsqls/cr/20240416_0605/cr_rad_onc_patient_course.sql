DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_rad_onc_patient_course.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Patient_Course		   	   ##
-- ##  TARGET  DATABASE	   : EDWCR	 						   ##
-- ##  SOURCE		   : EDWCR_staging.STG_DimPlan	   ##
-- ##	                                                                         ##
-- ##  INITIAL RELEASE	   : 								   ##
-- ##  PROJECT            	   : 	 		    				   ##
-- ##  ------------------------------------------------------------------------	   ##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=$Job_Name;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DimCourse');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY dp.dimsiteid,
                                               dp.dimcourseid) AS patient_course_sk,
                                     ra.patient_sk AS patient_sk,
                                     dp.dimlookupid_clinicalstatus AS clinical_status_id,
                                     dp.dimlookupid_treatmentintenttyp AS treatment_intent_type_id,
                                     ra.site_sk AS site_sk,
                                     dp.dimcourseid AS source_patient_course_id,
                                     substr(dp.courseid, 1, 20) AS course_id_text,
                                     CAST(trim(substr(dp.coursestartdatetime, 1, 19)) AS DATETIME) AS coursestartdatetime,
                                     dp.notxsessionplanned AS course_session_planned_num,
                                     dp.notxsessiondelivered AS course_session_delivered_num,
                                     dp.notxsessionremaining AS course_session_remaining_num,
                                     ROUND(CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dosedelivered) AS NUMERIC), 10, 'ROUND_HALF_EVEN') AS dose_delivered_amt,
                                     dp.courseduration AS course_duration_num,
                                     substr(dp.comment_comment, 1, 255) AS comment_text,
                                     dp.logid AS log_id,
                                     dp.runid AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimcourse AS dp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site AS rs ON rs.source_site_id = dp.dimsiteid
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient AS ra ON dp.dimpatientid = ra.source_patient_id
   AND rs.site_sk = ra.site_sk
   WHERE ra.site_sk IS NOT NULL ) AS ms ON mt.patient_course_sk = ms.patient_course_sk
AND (coalesce(mt.patient_sk, 0) = coalesce(ms.patient_sk, 0)
     AND coalesce(mt.patient_sk, 1) = coalesce(ms.patient_sk, 1))
AND (coalesce(mt.clinical_status_id, 0) = coalesce(ms.clinical_status_id, 0)
     AND coalesce(mt.clinical_status_id, 1) = coalesce(ms.clinical_status_id, 1))
AND (coalesce(mt.treatment_intent_type_id, 0) = coalesce(ms.treatment_intent_type_id, 0)
     AND coalesce(mt.treatment_intent_type_id, 1) = coalesce(ms.treatment_intent_type_id, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_patient_course_id = ms.source_patient_course_id
AND (upper(coalesce(mt.course_id_text, '0')) = upper(coalesce(ms.course_id_text, '0'))
     AND upper(coalesce(mt.course_id_text, '1')) = upper(coalesce(ms.course_id_text, '1')))
AND (coalesce(mt.course_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.coursestartdatetime, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.course_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.coursestartdatetime, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.course_session_planned_num, 0) = coalesce(ms.course_session_planned_num, 0)
     AND coalesce(mt.course_session_planned_num, 1) = coalesce(ms.course_session_planned_num, 1))
AND (coalesce(mt.course_session_delivered_num, 0) = coalesce(ms.course_session_delivered_num, 0)
     AND coalesce(mt.course_session_delivered_num, 1) = coalesce(ms.course_session_delivered_num, 1))
AND (coalesce(mt.course_session_remaining_num, 0) = coalesce(ms.course_session_remaining_num, 0)
     AND coalesce(mt.course_session_remaining_num, 1) = coalesce(ms.course_session_remaining_num, 1))
AND (coalesce(mt.dose_delivered_amt, NUMERIC '0') = coalesce(ms.dose_delivered_amt, NUMERIC '0')
     AND coalesce(mt.dose_delivered_amt, NUMERIC '1') = coalesce(ms.dose_delivered_amt, NUMERIC '1'))
AND (coalesce(mt.course_duration_num, 0) = coalesce(ms.course_duration_num, 0)
     AND coalesce(mt.course_duration_num, 1) = coalesce(ms.course_duration_num, 1))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_course_sk,
        patient_sk,
        clinical_status_id,
        treatment_intent_type_id,
        site_sk,
        source_patient_course_id,
        course_id_text,
        course_start_date_time,
        course_session_planned_num,
        course_session_delivered_num,
        course_session_remaining_num,
        dose_delivered_amt,
        course_duration_num,
        comment_text,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_course_sk, ms.patient_sk, ms.clinical_status_id, ms.treatment_intent_type_id, ms.site_sk, ms.source_patient_course_id, ms.course_id_text, ms.coursestartdatetime, ms.course_session_planned_num, ms.course_session_delivered_num, ms.course_session_remaining_num, ms.dose_delivered_amt, ms.course_duration_num, ms.comment_text, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_course_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course
      GROUP BY patient_course_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Rad_Onc_Patient_Course');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;