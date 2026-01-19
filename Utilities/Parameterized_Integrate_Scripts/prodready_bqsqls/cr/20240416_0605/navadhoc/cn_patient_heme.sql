DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #################################################################################
-- # Description: This script loads from CN_Patient_Heme_STG                       #
-- # into EDWCR.CN_Patient_Heme.                                                   #
-- #                                                                               #
-- # Author: Skylar Youngblood                                                     #
-- # Date: 06-18-2019                                                              #
-- #################################################################################
-- # Change control:                                                               #
-- #                                                                               #
-- # Skylar Youngblood 06-18-2019                        INITIAL RELEASE           #
-- #                                                                               #
-- #                                                                               #
-- #################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Heme;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Heme_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme
WHERE upper(rtrim(cn_patient_heme.hashbite_ssk)) NOT IN
    (SELECT upper(rtrim(cn_patient_heme_stg.hbsource)) AS hbsource
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_heme AS mt USING
  (SELECT DISTINCT stg.patienthemefactid,
                   CAST(stg.patientdimid AS NUMERIC) AS nav_patient_id,
                   stg.tumortypedimid,
                   stg.diagnosisresultid,
                   stg.diagnosisdimid,
                   max(stg.coid) AS coid,
                   'H' AS company_code,
                   stg.navigatordimid,
                   max(stg.transportation) AS transportation_text,
                   max(stg.drugusehistory) AS drug_use_history_text,
                   dtl.physician_id,
                   substr(max(stg.hbsource), 1, 60) AS hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_heme_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.cn_physician_detail AS dtl ON upper(rtrim(dtl.physician_name)) = upper(rtrim(stg.hematologist))
   WHERE upper(rtrim(stg.hbsource)) NOT IN
       (SELECT upper(rtrim(cn_patient_heme.hashbite_ssk)) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme)
   GROUP BY 1,
            2,
            3,
            4,
            5,
            upper(stg.coid),
            8,
            upper(stg.transportation),
            upper(stg.drugusehistory),
            11,
            upper(substr(stg.hbsource, 1, 60)),
            14 QUALIFY row_number() OVER (PARTITION BY stg.patienthemefactid
                                          ORDER BY dtl.physician_id DESC) = 1) AS ms ON mt.cn_patient_heme_sid = ms.patienthemefactid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumortypedimid, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumortypedimid, 1))
AND (coalesce(mt.diagnosis_result_id, 0) = coalesce(ms.diagnosisresultid, 0)
     AND coalesce(mt.diagnosis_result_id, 1) = coalesce(ms.diagnosisresultid, 1))
AND (coalesce(mt.nav_diagnosis_id, 0) = coalesce(ms.diagnosisdimid, 0)
     AND coalesce(mt.nav_diagnosis_id, 1) = coalesce(ms.diagnosisdimid, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigatordimid, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigatordimid, 1))
AND (upper(coalesce(mt.transportation_text, '0')) = upper(coalesce(ms.transportation_text, '0'))
     AND upper(coalesce(mt.transportation_text, '1')) = upper(coalesce(ms.transportation_text, '1')))
AND (upper(coalesce(mt.drug_use_history_text, '0')) = upper(coalesce(ms.drug_use_history_text, '0'))
     AND upper(coalesce(mt.drug_use_history_text, '1')) = upper(coalesce(ms.drug_use_history_text, '1')))
AND (coalesce(mt.hematologist_physician_id, 0) = coalesce(ms.physician_id, 0)
     AND coalesce(mt.hematologist_physician_id, 1) = coalesce(ms.physician_id, 1))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_heme_sid,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        coid,
        company_code,
        navigator_id,
        transportation_text,
        drug_use_history_text,
        hematologist_physician_id,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.patienthemefactid, ms.nav_patient_id, ms.tumortypedimid, ms.diagnosisresultid, ms.diagnosisdimid, ms.coid, ms.company_code, ms.navigatordimid, ms.transportation_text, ms.drug_use_history_text, ms.physician_id, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_heme_sid
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_heme
      GROUP BY cn_patient_heme_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_heme');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','CN_Patient_Heme');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF