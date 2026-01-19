DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_functional_assess.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #################################################################################
-- # Description: This script loads from CN_Patient_Heme_Functional_Assess_STG     #
-- # into EDWCR.CN_Patient_Heme_Func_Assess.                                       #
-- #                                                                               #
-- # Author: Skylar Youngblood                                                     #
-- # Date: 06-13-2019                                                              #
-- #################################################################################
-- # Change control:                                                               #
-- #                                                                               #
-- # Skylar Youngblood 06-13-2019                        INITIAL RELEASE           #
-- #                                                                               #
-- #                                                                               #
-- #################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_HEME_FUNCTIONAL_ASSESS;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Heme_Functional_Assess_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess
WHERE upper(rtrim(cn_patient_heme_func_assess.hashbite_ssk)) NOT IN
    (SELECT upper(rtrim(cn_patient_heme_functional_assess_stg.hbsource)) AS hbsource
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_functional_assess_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess AS mt USING
  (SELECT DISTINCT stg.patienthemefuncassessfactid AS cn_patient_heme_func_assess_sid,
                   CAST(stg.patientdimid AS NUMERIC) AS nav_patient_id,
                   stg.tumortypedimid AS tumor_type_id,
                   stg.diagnosisresultid AS diagnosis_result_id,
                   stg.diagnosisdimid AS nav_diagnosis_id,
                   stg.coid AS coid,
                   'H' AS company_code,
                   stg.navigatordimid,
                   stg.functionalassessmentdate,
                   ref.test_type_id,
                   stg.testtyperesults,
                   substr(stg.hbsource, 1, 60) AS hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_functional_assess_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.ref_test_type AS REF ON upper(rtrim(ref.test_sub_type_desc)) = upper(rtrim(stg.testtype))
   AND upper(rtrim(ref.test_type_desc)) = 'FUNCTIONAL ASSESSMENT'
   WHERE upper(rtrim(stg.hbsource)) NOT IN
       (SELECT upper(rtrim(cn_patient_heme_func_assess.hashbite_ssk)) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess) ) AS ms ON mt.cn_patient_heme_func_assess_sid = ms.cn_patient_heme_func_assess_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosis_result_id, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosis_result_id, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.nav_diagnosis_id, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.nav_diagnosis_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigatordimid, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigatordimid, 1))
AND (coalesce(mt.func_assess_date, DATE '1970-01-01') = coalesce(ms.functionalassessmentdate, DATE '1970-01-01')
     AND coalesce(mt.func_assess_date, DATE '1970-01-02') = coalesce(ms.functionalassessmentdate, DATE '1970-01-02'))
AND (coalesce(mt.test_type_id, 0) = coalesce(ms.test_type_id, 0)
     AND coalesce(mt.test_type_id, 1) = coalesce(ms.test_type_id, 1))
AND (coalesce(mt.test_type_result_amt, NUMERIC '0') = coalesce(ms.testtyperesults, NUMERIC '0')
     AND coalesce(mt.test_type_result_amt, NUMERIC '1') = coalesce(ms.testtyperesults, NUMERIC '1'))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_heme_func_assess_sid,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        coid,
        company_code,
        navigator_id,
        func_assess_date,
        test_type_id,
        test_type_result_amt,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_heme_func_assess_sid, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.coid, ms.company_code, ms.navigatordimid, ms.functionalassessmentdate, ms.test_type_id, ms.testtyperesults, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_heme_func_assess_sid
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess
      GROUP BY cn_patient_heme_func_assess_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Heme_Func_Assess');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF