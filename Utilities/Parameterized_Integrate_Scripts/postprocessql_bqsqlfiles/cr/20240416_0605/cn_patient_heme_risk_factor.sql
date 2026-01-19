DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_heme_risk_factor.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		        : EDWCR.CN_PATIENT_HEME_RISK_FACTOR	        #
-- #  TARGET  DATABASE	   	: EDWCR	 					#
-- #  SOURCE		   	: EDWCR_STAGING.PATIENT_HEME_RISK_FACTOR_STG
--                              	                                          	#
-- #	                                                                        #
-- #  INITIAL RELEASE	   	:Sandhya Jawagi  08-26-2019 						#
-- #  PROJECT                      : CANCER RESEARCH	 		    			#
-- #  ------------------------------------------------------------------------	#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_HEME_RISK_FACTOR_STG;;
 --' FOR SESSION;;
 -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','PATIENT_HEME_RISK_FACTOR_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor
WHERE upper(cn_patient_heme_risk_factor.hashbite_ssk) NOT IN
    (SELECT upper(patient_heme_risk_factor_stg.hashbite_ssk) AS hashbite_ssk
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_risk_factor_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor AS mt USING
  (SELECT DISTINCT t1.cn_patient_heme_diagnosis_sid,
                   t1.nav_patient_id,
                   t1.tumor_type_id,
                   t1.diagnosis_result_id,
                   t1.nav_diagnosis_id,
                   t1.coid AS coid,
                   t1.company_code AS company_code,
                   CAST(t1.navigator_id AS INT64) AS navigator_id,
                   substr(t1.risk_factor_text, 1, 255) AS risk_factor_text,
                   substr(t1.other_risk_factor_text, 1, 100) AS other_risk_factor_text,
                   t1.previous_tumor_site_id,
                   t1.other_previous_tumor_site_id,
                   t1.hashbite_ssk,
                   t1.source_system_code AS source_system_code,
                   t1.dw_last_update_date_time
   FROM
     (SELECT stg.patienthemediagnosisfactid AS cn_patient_heme_diagnosis_sid,
             stg.patientdimid AS nav_patient_id,
             stg.tumortypedimid AS tumor_type_id,
             stg.diagnosisresultid AS diagnosis_result_id,
             stg.diagnosisdimid AS nav_diagnosis_id,
             stg.coid AS coid,
             'H' AS company_code,
             stg.navigatordimid AS navigator_id,
             trim(stg.riskfactor) AS risk_factor_text,
             trim(stg.otherriskfactor) AS other_risk_factor_text,
             prev.site_location_id AS previous_tumor_site_id,
             oth_prev.site_location_id AS other_previous_tumor_site_id,
             stg.hashbite_ssk,
             'N' AS source_system_code,
             datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
      FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_risk_factor_stg AS stg
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_site_location AS oth_prev ON upper(rtrim(stg.othertumordiseasesite)) = upper(rtrim(oth_prev.site_location_desc))
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_site_location AS prev ON upper(rtrim(stg.tumordiseasesite)) = upper(rtrim(prev.site_location_desc))
      LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor AS tgt ON upper(rtrim(stg.hashbite_ssk)) = upper(rtrim(tgt.hashbite_ssk))
      WHERE tgt.hashbite_ssk IS NULL ) AS t1) AS ms ON mt.cn_patient_heme_diagnosis_sid = ms.cn_patient_heme_diagnosis_sid
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
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND (upper(coalesce(mt.risk_factor_text, '0')) = upper(coalesce(ms.risk_factor_text, '0'))
     AND upper(coalesce(mt.risk_factor_text, '1')) = upper(coalesce(ms.risk_factor_text, '1')))
AND (upper(coalesce(mt.other_risk_factor_text, '0')) = upper(coalesce(ms.other_risk_factor_text, '0'))
     AND upper(coalesce(mt.other_risk_factor_text, '1')) = upper(coalesce(ms.other_risk_factor_text, '1')))
AND (coalesce(mt.previous_tumor_site_id, 0) = coalesce(ms.previous_tumor_site_id, 0)
     AND coalesce(mt.previous_tumor_site_id, 1) = coalesce(ms.previous_tumor_site_id, 1))
AND (coalesce(mt.other_previous_tumor_site_id, 0) = coalesce(ms.other_previous_tumor_site_id, 0)
     AND coalesce(mt.other_previous_tumor_site_id, 1) = coalesce(ms.other_previous_tumor_site_id, 1))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_heme_diagnosis_sid,
        nav_patient_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        coid,
        company_code,
        navigator_id,
        risk_factor_text,
        other_risk_factor_text,
        previous_tumor_site_id,
        other_previous_tumor_site_id,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_heme_diagnosis_sid, ms.nav_patient_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.coid, ms.company_code, ms.navigator_id, ms.risk_factor_text, ms.other_risk_factor_text, ms.previous_tumor_site_id, ms.other_previous_tumor_site_id, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_heme_diagnosis_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor
      GROUP BY cn_patient_heme_diagnosis_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_risk_factor');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

-- and TRIM(Risk_Factor_Text)<>Risk_Factor_Text
IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','cn_patient_heme_risk_factor');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF