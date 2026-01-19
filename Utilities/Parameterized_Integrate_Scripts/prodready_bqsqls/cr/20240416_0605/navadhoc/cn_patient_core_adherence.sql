DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_core_adherence.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_CORE_ADHERENCE	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_PATIENT_CORE_ADHERENCE_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_CORE_ADHERENCE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_CORE_ADHERENCE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence
WHERE upper(cn_patient_core_adherence.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_core_adherence_stg.hashbite_ssk) AS hashbite_ssk
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_core_adherence_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence AS mt USING
  (SELECT DISTINCT stg.cn_patient_core_adherence_sid,
                   stg.core_adherence_measure_id,
                   stg.nav_patient_id,
                   stg.tumor_type_id,
                   stg.diagnosis_result_id,
                   stg.nav_diagnosis_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.core_adherence_measure_text,
                   stg.hashbite_ssk,
                   stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_core_adherence_stg AS stg
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_core_adherence.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence) ) AS ms ON mt.cn_patient_core_adherence_sid = ms.cn_patient_core_adherence_sid
AND (coalesce(mt.core_adherence_measure_id, 0) = coalesce(ms.core_adherence_measure_id, 0)
     AND coalesce(mt.core_adherence_measure_id, 1) = coalesce(ms.core_adherence_measure_id, 1))
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
AND (upper(coalesce(mt.core_adherence_measure_text, '0')) = upper(coalesce(ms.core_adherence_measure_text, '0'))
     AND upper(coalesce(mt.core_adherence_measure_text, '1')) = upper(coalesce(ms.core_adherence_measure_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_core_adherence_sid,
        core_adherence_measure_id,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        core_adherence_measure_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_core_adherence_sid, ms.core_adherence_measure_id, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.core_adherence_measure_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_core_adherence_sid,
             core_adherence_measure_id
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence
      GROUP BY cn_patient_core_adherence_sid,
               core_adherence_measure_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_CORE_ADHERENCE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF