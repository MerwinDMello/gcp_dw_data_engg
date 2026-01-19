DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cr_rad_onc_patient_plan.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.Rad_Onc_Patient_Plan		   	   ##
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
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','STG_DimPlan');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_plan;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_plan AS mt USING
  (SELECT DISTINCT row_number() OVER (
                                      ORDER BY upper(dp.dimsiteid),
                                               upper(dp.dimplanid)) AS patient_plan_sk,
                                     rpp.plan_purpose_sk,
                                     rr.site_sk AS site_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimplanid) AS INT64) AS source_patient_plan_id,
                                     rpc.patient_course_sk,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimlookupid_planstatus) AS INT64) AS plan_status_id,
                                     substr(dp.planuid, 1, 65) AS plan_unique_id_text,
                                     CAST(trim(substr(dp.creationdate, 1, 19)) AS DATETIME) AS history_date_time,
                                     CAST(trim(substr(dp.firstdayoftreatment, 1, 19)) AS DATETIME) AS history_date_time_cw_1,
                                     CAST(trim(substr(dp.lastdayoftreatment, 1, 19)) AS DATETIME) AS history_date_time_cw_2,
                                     substr(dp.hstryusername, 1, 30) AS history_user_name,
                                     CAST(trim(substr(dp.hstrydatetime, 1, 19)) AS DATETIME) AS history_date_time_cw_3,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.logid) AS INT64) AS log_id,
                                     CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.runid) AS INT64) AS run_id,
                                     'R' AS source_system_code,
                                     datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.stg_dimplan AS dp
   INNER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_site AS rr ON rr.source_site_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimsiteid) AS FLOAT64)
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.ref_rad_onc_plan_purpose AS rpp ON upper(rtrim(dp.planintent)) = upper(rtrim(rpp.plan_purpose_name))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_course AS rpc ON rpc.source_patient_course_id = CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(dp.dimcourseid) AS FLOAT64)
   AND rpc.site_sk = rr.site_sk) AS ms ON mt.patient_plan_sk = ms.patient_plan_sk
AND (coalesce(mt.plan_purpose_sk, 0) = coalesce(ms.plan_purpose_sk, 0)
     AND coalesce(mt.plan_purpose_sk, 1) = coalesce(ms.plan_purpose_sk, 1))
AND mt.site_sk = ms.site_sk
AND mt.source_patient_plan_id = ms.source_patient_plan_id
AND (coalesce(mt.patient_course_sk, 0) = coalesce(ms.patient_course_sk, 0)
     AND coalesce(mt.patient_course_sk, 1) = coalesce(ms.patient_course_sk, 1))
AND (coalesce(mt.plan_status_id, 0) = coalesce(ms.plan_status_id, 0)
     AND coalesce(mt.plan_status_id, 1) = coalesce(ms.plan_status_id, 1))
AND (upper(coalesce(mt.plan_unique_id_text, '0')) = upper(coalesce(ms.plan_unique_id_text, '0'))
     AND upper(coalesce(mt.plan_unique_id_text, '1')) = upper(coalesce(ms.plan_unique_id_text, '1')))
AND (coalesce(mt.plan_creation_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.plan_creation_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.treatment_start_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time_cw_1, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.treatment_start_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time_cw_1, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.treatment_end_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time_cw_2, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.treatment_end_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time_cw_2, DATETIME '1970-01-01 00:00:01'))
AND (upper(coalesce(mt.history_user_name, '0')) = upper(coalesce(ms.history_user_name, '0'))
     AND upper(coalesce(mt.history_user_name, '1')) = upper(coalesce(ms.history_user_name, '1')))
AND (coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:00') = coalesce(ms.history_date_time_cw_3, DATETIME '1970-01-01 00:00:00')
     AND coalesce(mt.history_date_time, DATETIME '1970-01-01 00:00:01') = coalesce(ms.history_date_time_cw_3, DATETIME '1970-01-01 00:00:01'))
AND (coalesce(mt.log_id, 0) = coalesce(ms.log_id, 0)
     AND coalesce(mt.log_id, 1) = coalesce(ms.log_id, 1))
AND (coalesce(mt.run_id, 0) = coalesce(ms.run_id, 0)
     AND coalesce(mt.run_id, 1) = coalesce(ms.run_id, 1))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (patient_plan_sk,
        plan_purpose_sk,
        site_sk,
        source_patient_plan_id,
        patient_course_sk,
        plan_status_id,
        plan_unique_id_text,
        plan_creation_date_time,
        treatment_start_date_time,
        treatment_end_date_time,
        history_user_name,
        history_date_time,
        log_id,
        run_id,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patient_plan_sk, ms.plan_purpose_sk, ms.site_sk, ms.source_patient_plan_id, ms.patient_course_sk, ms.plan_status_id, ms.plan_unique_id_text, ms.history_date_time, ms.history_date_time_cw_1, ms.history_date_time_cw_2, ms.history_user_name, ms.history_date_time_cw_3, ms.log_id, ms.run_id, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT patient_plan_sk
      FROM `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_plan
      GROUP BY patient_plan_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.rad_onc_patient_plan');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','Rad_Onc_Patient_Plan');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF