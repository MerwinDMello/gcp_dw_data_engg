DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_clinical_trial.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #################################################################################
-- # Description: This script loads from CN_Patient_Heme_STG                       #
-- # into EDWCR.CN_Patient_Heme.                                                   #
-- #                                                                               #
-- # Author: Bhagyashree Kademani                                                  #
-- # Date: 09-13-2019                                                              #
-- #################################################################################
-- # Change control:                                                               #
-- #                                                                               #
-- #                                                     INITIAL RELEASE           #
-- #                                                                               #
-- #                                                                               #
-- #################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Heme_Clinical_Trial_stg;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Heme_Clinical_Trial_stg');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_clinical_trial AS mt USING
  (SELECT DISTINCT iq.cn_patient_heme_clinical_trial_sid AS cn_patient_heme_clinical_trial_sid,
                   CAST(iq.nav_patient_id AS NUMERIC) AS nav_patient_id,
                   iq.tumor_type_id AS tumor_type_id,
                   iq.diagnosis_result_id AS diagnosis_result_id,
                   iq.nav_diagnosis_id AS nav_diagnosis_id,
                   iq.navigator_id AS navigator_id,
                   iq.coid AS coid,
                   iq.company_code AS company_code,
                   iq.clinical_trial_evaluated_ind AS clinical_trial_evaluated_ind,
                   CAST(trim(iq.clinical_trial_evaluated_date) AS DATE) AS clinical_trial_evaluated_date,
                   iq.clinical_trial_enrolled_ind AS clinical_trial_enrolled_ind,
                   CAST(trim(iq.clinical_trial_enrolled_date) AS DATE) AS clinical_trial_enrolled_date,
                   iq.clinical_trial_offered_ind AS clinical_trial_offered_ind,
                   CAST(trim(iq.clinical_trial_offered_date) AS DATE) AS clinical_trial_offered_date,
                   iq.clinical_trial_not_offered_text,
                   iq.clinical_trial_not_offered_other_text,
                   iq.clinical_trial_name AS clinical_trial_name,
                   iq.clinical_trial_other_name,
                   iq.not_screened_reason_text,
                   iq.not_screened_other_reason_text,
                   iq.hashbite_ssk,
                   iq.source_system_code AS source_system_code,
                   iq.dw_last_update_date_time
   FROM
     (SELECT stg.patientclinicaltrialfactid AS cn_patient_heme_clinical_trial_sid,
             stg.patientdimid AS nav_patient_id,
             stg.tumortypedimid AS tumor_type_id,
             stg.diagnosisresultid AS diagnosis_result_id,
             stg.diagnosisdimid AS nav_diagnosis_id,
             stg.navigatordimid AS navigator_id,
             stg.coid,
             'H' AS company_code,
             CASE upper(trim(stg.hemeevaluated_screenedind))
                 WHEN 'YES' THEN 'Y'
                 WHEN 'NO' THEN 'N'
                 ELSE CAST(NULL AS STRING)
             END AS clinical_trial_evaluated_ind,
             stg.hemeevaluated_screeneddate AS clinical_trial_evaluated_date,
             CASE upper(trim(stg.hemepatientenrolledinclinicaltrial))
                 WHEN 'YES' THEN 'Y'
                 WHEN 'NO' THEN 'N'
                 ELSE CAST(NULL AS STRING)
             END AS clinical_trial_enrolled_ind,
             stg.hemeenrolledinclinicaltrialdate AS clinical_trial_enrolled_date,
             CASE upper(trim(stg.hemeofferedclinicaltrialind))
                 WHEN 'YES' THEN 'Y'
                 WHEN 'NO' THEN 'N'
                 ELSE CAST(NULL AS STRING)
             END AS clinical_trial_offered_ind,
             stg.hemeofferedclinicaltrialdate AS clinical_trial_offered_date,
             stg.hemetrialnotoffered AS clinical_trial_not_offered_text,
             stg.hemetrialnotofferedother AS clinical_trial_not_offered_other_text,
             stg.hemeclinicaltrialname AS clinical_trial_name,
             stg.hemeclinicaltrialothername AS clinical_trial_other_name,
             stg.hemenotscreened AS not_screened_reason_text,
             stg.hemenotscreenedother AS not_screened_other_reason_text,
             stg.hbsource AS hashbite_ssk,
             'N' AS source_system_code,
             stg.dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_heme_clinical_trial_stg AS stg) AS iq
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_clinical_trial AS cphc ON upper(trim(iq.hashbite_ssk)) = upper(trim(cphc.hashbite_ssk))
   WHERE trim(cphc.hashbite_ssk) IS NULL ) AS ms ON mt.cn_patient_heme_clinical_trial_sid = ms.cn_patient_heme_clinical_trial_sid
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
AND (upper(coalesce(mt.clinical_trial_evaluated_ind, '0')) = upper(coalesce(ms.clinical_trial_evaluated_ind, '0'))
     AND upper(coalesce(mt.clinical_trial_evaluated_ind, '1')) = upper(coalesce(ms.clinical_trial_evaluated_ind, '1')))
AND (coalesce(mt.clinical_trial_evaluated_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_evaluated_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_evaluated_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_evaluated_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.clinical_trial_enrolled_ind, '0')) = upper(coalesce(ms.clinical_trial_enrolled_ind, '0'))
     AND upper(coalesce(mt.clinical_trial_enrolled_ind, '1')) = upper(coalesce(ms.clinical_trial_enrolled_ind, '1')))
AND (coalesce(mt.clinical_trial_enrolled_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_enrolled_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_enrolled_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_enrolled_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.clinical_trial_offered_ind, '0')) = upper(coalesce(ms.clinical_trial_offered_ind, '0'))
     AND upper(coalesce(mt.clinical_trial_offered_ind, '1')) = upper(coalesce(ms.clinical_trial_offered_ind, '1')))
AND (coalesce(mt.clinical_trial_offered_date, DATE '1970-01-01') = coalesce(ms.clinical_trial_offered_date, DATE '1970-01-01')
     AND coalesce(mt.clinical_trial_offered_date, DATE '1970-01-02') = coalesce(ms.clinical_trial_offered_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.clinical_trial_not_offered_text, '0')) = upper(coalesce(ms.clinical_trial_not_offered_text, '0'))
     AND upper(coalesce(mt.clinical_trial_not_offered_text, '1')) = upper(coalesce(ms.clinical_trial_not_offered_text, '1')))
AND (upper(coalesce(mt.clinical_trial_not_offered_other_text, '0')) = upper(coalesce(ms.clinical_trial_not_offered_other_text, '0'))
     AND upper(coalesce(mt.clinical_trial_not_offered_other_text, '1')) = upper(coalesce(ms.clinical_trial_not_offered_other_text, '1')))
AND (upper(coalesce(mt.clinical_trial_name, '0')) = upper(coalesce(ms.clinical_trial_name, '0'))
     AND upper(coalesce(mt.clinical_trial_name, '1')) = upper(coalesce(ms.clinical_trial_name, '1')))
AND (upper(coalesce(mt.clinical_trial_other_name, '0')) = upper(coalesce(ms.clinical_trial_other_name, '0'))
     AND upper(coalesce(mt.clinical_trial_other_name, '1')) = upper(coalesce(ms.clinical_trial_other_name, '1')))
AND (upper(coalesce(mt.not_screened_reason_text, '0')) = upper(coalesce(ms.not_screened_reason_text, '0'))
     AND upper(coalesce(mt.not_screened_reason_text, '1')) = upper(coalesce(ms.not_screened_reason_text, '1')))
AND (upper(coalesce(mt.not_screened_other_reason_text, '0')) = upper(coalesce(ms.not_screened_other_reason_text, '0'))
     AND upper(coalesce(mt.not_screened_other_reason_text, '1')) = upper(coalesce(ms.not_screened_other_reason_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_heme_clinical_trial_sid,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        clinical_trial_evaluated_ind,
        clinical_trial_evaluated_date,
        clinical_trial_enrolled_ind,
        clinical_trial_enrolled_date,
        clinical_trial_offered_ind,
        clinical_trial_offered_date,
        clinical_trial_not_offered_text,
        clinical_trial_not_offered_other_text,
        clinical_trial_name,
        clinical_trial_other_name,
        not_screened_reason_text,
        not_screened_other_reason_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_heme_clinical_trial_sid, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.clinical_trial_evaluated_ind, ms.clinical_trial_evaluated_date, ms.clinical_trial_enrolled_ind, ms.clinical_trial_enrolled_date, ms.clinical_trial_offered_ind, ms.clinical_trial_offered_date, ms.clinical_trial_not_offered_text, ms.clinical_trial_not_offered_other_text, ms.clinical_trial_name, ms.clinical_trial_other_name, ms.not_screened_reason_text, ms.not_screened_other_reason_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_heme_clinical_trial_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_clinical_trial
      GROUP BY cn_patient_heme_clinical_trial_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_clinical_trial');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- Deleting Records From Core table if matching Hashbite_SSK not coming from source
BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_clinical_trial AS cphc
WHERE NOT EXISTS
    (SELECT 'X'
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.cn_patient_heme_clinical_trial_stg AS sphc
     WHERE upper(trim(sphc.hbsource)) = upper(trim(cphc.hashbite_ssk)) );


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Heme_Clinical_Trial');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF;;