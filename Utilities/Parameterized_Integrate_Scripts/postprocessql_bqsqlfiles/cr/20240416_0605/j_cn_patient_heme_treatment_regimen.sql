DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/j_cn_patient_heme_treatment_regimen.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_HEME_TREATMENT_REGIMEN	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.Patient_Heme_Treatment_Regimen_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_HEME_TREATMENT_REGIMEN;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','Patient_Heme_Treatment_Regimen_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_treatment_regimen
WHERE upper(rtrim(cn_patient_heme_treatment_regimen.hashbite_ssk)) NOT IN
    (SELECT upper(rtrim(patient_heme_treatment_regimen_stg.hbsource)) AS hbsource
     FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_treatment_regimen_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_treatment_regimen AS mt USING
  (SELECT DISTINCT CAST(stg.patienthemediagnosisfactid AS INT64) AS cn_patient_heme_diagnosis_sid,
                   CAST(ROUND(stg.patientdimid, 0, 'ROUND_HALF_EVEN') AS NUMERIC) AS nav_patient_id,
                   rs.regimen_id,
                   tr.treatment_phase_id,
                   pr.pathway_var_reason_id,
                   CAST(stg.tumortypedimid AS INT64) AS tumor_type_id,
                   CAST(stg.diagnosisresultid AS INT64) AS diagnosis_result_id,
                   CAST(stg.diagnosisdimid AS INT64) AS nav_diagnosis_id,
                   CAST(stg.navigatordimid AS INT64) AS navigator_id,
                   trim(stg.coid) AS coid,
                   'H' AS company_code,
                   parse_date('%Y-%m-%d', substr(stg.plannedstartdate, 1, 19)) AS planned_start_date,
                   parse_date('%Y-%m-%d', substr(stg.actualstartdate, 1, 19)) AS actual_start_date,
                   substr(trim(stg.drugs), 1, 100) AS drug_text,
                   stg.cycles AS cycle_num,
                   CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg.cyclelength) AS INT64) AS cycle_length_num,
                   substr(trim(stg.cyclelengthinterval), 1, 20) AS cycle_frequency_text,
                   CAST(`hca-hin-dev-cur-ops`.bqutil_fns.cw_td_normalize_number(stg.cyclenumber) AS INT64) AS ordinal_cycle_num,
                   CASE
                       WHEN upper(trim(stg.pathwayyesno)) = 'ON PATHWAY' THEN 'Y'
                       ELSE 'N'
                   END AS pathway_ind,
                   substr(trim(stg.pathwayname), 1, 50) AS pathway_text,
                   CASE
                       WHEN upper(trim(stg.pathwaycompliant)) = 'COMPLIANT' THEN 'Y'
                       ELSE 'N'
                   END AS pathway_compliant_ind,
                   parse_date('%Y-%m-%d', substr(stg.treatmentplandoccumentdate, 1, 19)) AS treatment_plan_document_date,
                   CASE
                       WHEN upper(trim(stg.plandocumentedtimeframe)) = 'PLAN NOT DOCUMENTED PRIOR TO TREATMENT' THEN 'N'
                       ELSE 'Y'
                   END AS prior_plan_document_timeframe_ind,
                   substr(trim(stg.treatmentregimencoments), 1, 1000) AS treatment_regimen_comment_text,
                   substr(trim(stg.hbsource), 1, 60) AS hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM `hca-hin-dev-cur-ops`.edwcr_staging.patient_heme_treatment_regimen_stg AS stg
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_regimen AS rs ON upper(trim(stg.regimen)) = upper(trim(rs.regimen_name))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_treatment_phase AS tr ON upper(trim(stg.treatmentphase)) = upper(trim(tr.treatment_phase_desc))
   LEFT OUTER JOIN `hca-hin-dev-cur-ops`.edwcr_base_views.ref_pathway_var_reason AS pr ON upper(trim(stg.pathwayvariancereason)) = upper(trim(pr.pathway_var_reason_type_desc))
   AND upper(trim(coalesce(stg.otherpathwayreason, '##'))) = upper(trim(coalesce(pr.pathway_var_reason_sub_type_desc, '##')))
   WHERE upper(trim(stg.hbsource)) NOT IN
       (SELECT upper(trim(cn_patient_heme_treatment_regimen.hashbite_ssk))
        FROM `hca-hin-dev-cur-ops`.edwcr_base_views.cn_patient_heme_treatment_regimen) ) AS ms ON mt.cn_patient_heme_diagnosis_sid = ms.cn_patient_heme_diagnosis_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.regimen_id, 0) = coalesce(ms.regimen_id, 0)
     AND coalesce(mt.regimen_id, 1) = coalesce(ms.regimen_id, 1))
AND (coalesce(mt.treatment_phase_id, 0) = coalesce(ms.treatment_phase_id, 0)
     AND coalesce(mt.treatment_phase_id, 1) = coalesce(ms.treatment_phase_id, 1))
AND (coalesce(mt.pathway_var_reason_id, 0) = coalesce(ms.pathway_var_reason_id, 0)
     AND coalesce(mt.pathway_var_reason_id, 1) = coalesce(ms.pathway_var_reason_id, 1))
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
AND (coalesce(mt.planned_start_date, DATE '1970-01-01') = coalesce(ms.planned_start_date, DATE '1970-01-01')
     AND coalesce(mt.planned_start_date, DATE '1970-01-02') = coalesce(ms.planned_start_date, DATE '1970-01-02'))
AND (coalesce(mt.actual_start_date, DATE '1970-01-01') = coalesce(ms.actual_start_date, DATE '1970-01-01')
     AND coalesce(mt.actual_start_date, DATE '1970-01-02') = coalesce(ms.actual_start_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.drug_text, '0')) = upper(coalesce(ms.drug_text, '0'))
     AND upper(coalesce(mt.drug_text, '1')) = upper(coalesce(ms.drug_text, '1')))
AND (coalesce(mt.cycle_num, 0) = coalesce(ms.cycle_num, 0)
     AND coalesce(mt.cycle_num, 1) = coalesce(ms.cycle_num, 1))
AND (coalesce(mt.cycle_length_num, 0) = coalesce(ms.cycle_length_num, 0)
     AND coalesce(mt.cycle_length_num, 1) = coalesce(ms.cycle_length_num, 1))
AND (upper(coalesce(mt.cycle_frequency_text, '0')) = upper(coalesce(ms.cycle_frequency_text, '0'))
     AND upper(coalesce(mt.cycle_frequency_text, '1')) = upper(coalesce(ms.cycle_frequency_text, '1')))
AND (coalesce(mt.ordinal_cycle_num, 0) = coalesce(ms.ordinal_cycle_num, 0)
     AND coalesce(mt.ordinal_cycle_num, 1) = coalesce(ms.ordinal_cycle_num, 1))
AND (upper(coalesce(mt.pathway_ind, '0')) = upper(coalesce(ms.pathway_ind, '0'))
     AND upper(coalesce(mt.pathway_ind, '1')) = upper(coalesce(ms.pathway_ind, '1')))
AND (upper(coalesce(mt.pathway_text, '0')) = upper(coalesce(ms.pathway_text, '0'))
     AND upper(coalesce(mt.pathway_text, '1')) = upper(coalesce(ms.pathway_text, '1')))
AND (upper(coalesce(mt.pathway_compliant_ind, '0')) = upper(coalesce(ms.pathway_compliant_ind, '0'))
     AND upper(coalesce(mt.pathway_compliant_ind, '1')) = upper(coalesce(ms.pathway_compliant_ind, '1')))
AND (coalesce(mt.treatment_plan_document_date, DATE '1970-01-01') = coalesce(ms.treatment_plan_document_date, DATE '1970-01-01')
     AND coalesce(mt.treatment_plan_document_date, DATE '1970-01-02') = coalesce(ms.treatment_plan_document_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.prior_plan_document_timeframe_ind, '0')) = upper(coalesce(ms.prior_plan_document_timeframe_ind, '0'))
     AND upper(coalesce(mt.prior_plan_document_timeframe_ind, '1')) = upper(coalesce(ms.prior_plan_document_timeframe_ind, '1')))
AND (upper(coalesce(mt.treatment_regimen_comment_text, '0')) = upper(coalesce(ms.treatment_regimen_comment_text, '0'))
     AND upper(coalesce(mt.treatment_regimen_comment_text, '1')) = upper(coalesce(ms.treatment_regimen_comment_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_heme_diagnosis_sid,
        nav_patient_id,
        regimen_id,
        treatment_phase_id,
        pathway_var_reason_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        planned_start_date,
        actual_start_date,
        drug_text,
        cycle_num,
        cycle_length_num,
        cycle_frequency_text,
        ordinal_cycle_num,
        pathway_ind,
        pathway_text,
        pathway_compliant_ind,
        treatment_plan_document_date,
        prior_plan_document_timeframe_ind,
        treatment_regimen_comment_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_heme_diagnosis_sid, ms.nav_patient_id, ms.regimen_id, ms.treatment_phase_id, ms.pathway_var_reason_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.planned_start_date, ms.actual_start_date, ms.drug_text, ms.cycle_num, ms.cycle_length_num, ms.cycle_frequency_text, ms.ordinal_cycle_num, ms.pathway_ind, ms.pathway_text, ms.pathway_compliant_ind, ms.treatment_plan_document_date, ms.prior_plan_document_timeframe_ind, ms.treatment_regimen_comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_heme_diagnosis_sid
      FROM `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_treatment_regimen
      GROUP BY cn_patient_heme_diagnosis_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table `hca-hin-dev-cur-ops`.edwcr.cn_patient_heme_treatment_regimen');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Heme_Treatment_Regimen');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF