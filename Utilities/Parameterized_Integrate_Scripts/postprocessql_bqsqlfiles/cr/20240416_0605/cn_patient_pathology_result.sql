DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_pathology_result.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_PATHOLOGY_RESULT	              	#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_PATIENT_PATHOLOGY_RESULT_STG#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 							#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              		#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_PATHOLOGY_RESULT;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_PATHOLOGY_RESULT_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_pathology_result
WHERE upper(trim(cn_patient_pathology_result.hashbite_ssk)) NOT IN
    (SELECT upper(trim(cn_patient_pathology_result_stg.hashbite_ssk))
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_pathology_result_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_pathology_result AS mt USING
  (SELECT DISTINCT cn_patient_pathology_result_stg.cn_patient_pathology_res_sid,
                   cn_patient_pathology_result_stg.pathology_result_type_id,
                   cn_patient_pathology_result_stg.core_record_type_id,
                   cn_patient_pathology_result_stg.nav_patient_id,
                   cn_patient_pathology_result_stg.tumor_type_id,
                   cn_patient_pathology_result_stg.diagnosis_result_id,
                   cn_patient_pathology_result_stg.nav_diagnosis_id,
                   cn_patient_pathology_result_stg.navigator_id,
                   cn_patient_pathology_result_stg.coid AS coid,
                   cn_patient_pathology_result_stg.company_code AS company_code,
                   cn_patient_pathology_result_stg.result_value_text,
                   cn_patient_pathology_result_stg.hashbite_ssk,
                   cn_patient_pathology_result_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_pathology_result_stg
   WHERE upper(cn_patient_pathology_result_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_pathology_result.hashbite_ssk) AS hashbite_ssk
        FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_pathology_result) ) AS ms ON mt.cn_patient_pathology_result_sid = ms.cn_patient_pathology_res_sid
AND (coalesce(mt.pathology_result_type_id, 0) = coalesce(ms.pathology_result_type_id, 0)
     AND coalesce(mt.pathology_result_type_id, 1) = coalesce(ms.pathology_result_type_id, 1))
AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.core_record_type_id, 0)
     AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.core_record_type_id, 1))
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (upper(coalesce(mt.result_value_text, '0')) = upper(coalesce(ms.result_value_text, '0'))
     AND upper(coalesce(mt.result_value_text, '1')) = upper(coalesce(ms.result_value_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_pathology_result_sid,
        pathology_result_type_id,
        core_record_type_id,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        result_value_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_pathology_res_sid, ms.pathology_result_type_id, ms.core_record_type_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.result_value_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_pathology_result_sid,
             pathology_result_type_id
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_pathology_result
      GROUP BY cn_patient_pathology_result_sid,
               pathology_result_type_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_pathology_result');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_PATHOLOGY_RESULT');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF