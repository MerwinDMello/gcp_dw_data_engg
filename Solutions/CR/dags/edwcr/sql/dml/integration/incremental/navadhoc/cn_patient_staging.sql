DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_staging.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ##################################################################################
-- ##  TARGET TABLE       	   : EDWCR.CN_PATIENT_STAGING		                ##
-- ##  TARGET  DATABASE	   : EDWCR	 					##
-- ##  SOURCE		   : EDWCR_staging.CN_PATIENT_STAGING_Stg		##
-- ##	                                                                        ##
-- ##  INITIAL RELEASE	   : 							##
-- ##  PROJECT            	   : 	 		    				##
-- ##  ------------------------------------------------------------------------	##
-- ##                                                                              ##
-- ##################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_STAGING;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_STAGING_Stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_staging
WHERE (upper(cn_patient_staging.hashbite_ssk),
       upper(cn_patient_staging.cancer_stage_class_method_code)) NOT IN
    (SELECT AS STRUCT upper(cn_patient_staging_stg.hashbite_ssk) AS hashbite_ssk,
                      upper(cn_patient_staging_stg.cancer_stage_class_method_code) AS cancer_stage_class_method_code
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_staging_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_staging AS mt USING
  (SELECT DISTINCT cn_patient_staging_stg.cn_patient_staging_sid,
                   cn_patient_staging_stg.diagnosis_result_id,
                   cn_patient_staging_stg.nav_patient_id,
                   cn_patient_staging_stg.tumor_type_id,
                   cn_patient_staging_stg.nav_diagnosis_id,
                   cn_patient_staging_stg.navigator_id,
                   cn_patient_staging_stg.coid AS coid,
                   cn_patient_staging_stg.company_code AS company_code,
                   cn_patient_staging_stg.cancer_stage_class_method_code AS cancer_stage_class_method_code,
                   cn_patient_staging_stg.cancer_staging_type_code AS cancer_staging_type_code,
                   cn_patient_staging_stg.cancer_staging_result_code,
                   cn_patient_staging_stg.cancer_stage_code,
                   cn_patient_staging_stg.hashbite_ssk,
                   cn_patient_staging_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_staging_stg
   WHERE (upper(cn_patient_staging_stg.hashbite_ssk),
          upper(cn_patient_staging_stg.cancer_stage_class_method_code)) NOT IN
       (SELECT AS STRUCT upper(cn_patient_staging.hashbite_ssk) AS hashbite_ssk,
                         upper(cn_patient_staging.cancer_stage_class_method_code) AS cancer_stage_class_method_code
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_staging) ) AS ms ON mt.cn_patient_staging_sid = ms.cn_patient_staging_sid
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND (upper(coalesce(mt.company_code, '0')) = upper(coalesce(ms.company_code, '0'))
     AND upper(coalesce(mt.company_code, '1')) = upper(coalesce(ms.company_code, '1')))
AND mt.cancer_stage_class_method_code = ms.cancer_stage_class_method_code
AND (upper(coalesce(mt.cancer_staging_type_code, '0')) = upper(coalesce(ms.cancer_staging_type_code, '0'))
     AND upper(coalesce(mt.cancer_staging_type_code, '1')) = upper(coalesce(ms.cancer_staging_type_code, '1')))
AND (upper(coalesce(mt.cancer_staging_result_code, '0')) = upper(coalesce(ms.cancer_staging_result_code, '0'))
     AND upper(coalesce(mt.cancer_staging_result_code, '1')) = upper(coalesce(ms.cancer_staging_result_code, '1')))
AND (upper(coalesce(mt.cancer_stage_code, '0')) = upper(coalesce(ms.cancer_stage_code, '0'))
     AND upper(coalesce(mt.cancer_stage_code, '1')) = upper(coalesce(ms.cancer_stage_code, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_staging_sid,
        diagnosis_result_id,
        nav_patient_id,
        tumor_type_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        cancer_stage_class_method_code,
        cancer_staging_type_code,
        cancer_staging_result_code,
        cancer_stage_code,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_staging_sid, ms.diagnosis_result_id, ms.nav_patient_id, ms.tumor_type_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.cancer_stage_class_method_code, ms.cancer_staging_type_code, ms.cancer_staging_result_code, ms.cancer_stage_code, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_staging_sid,
             cancer_stage_class_method_code,
             cancer_staging_type_code
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_staging
      GROUP BY cn_patient_staging_sid,
               cancer_stage_class_method_code,
               cancer_staging_type_code
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_staging');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Staging');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF