DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_complication.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_Patient_Complication	             		#
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_STAGING.CN_Patient_Complication_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_COMPLICATION;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_Patient_Complication_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_complication
WHERE upper(cn_patient_complication.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_complication_stg.hashbite_ssk) AS hashbite_ssk
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_complication_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_complication AS mt USING
  (SELECT DISTINCT stg.cn_patient_complication_sid,
                   stg.nav_patient_id,
                   stg.core_record_type_id,
                   stg.tumor_type_id,
                   stg.diagnosis_result_id,
                   stg.nav_diagnosis_id,
                   stg.navigator_id,
                   stg.coid AS coid,
                   'H' AS company_code,
                   stg.complication_date,
                   rtt.therapy_type_id,
                   stg.treatment_stopped_ind AS treatment_stopped_ind,
                   rs.nav_result_id AS outcome_result_id,
                   stg.complication_text,
                   stg.specific_complication_text,
                   stg.comment_text,
                   stg.hashbite_ssk,
                   'N' AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_complication_stg AS stg
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_therapy_type AS rtt ON upper(rtrim(coalesce(trim(stg.associatetherapytype), 'X'))) = upper(rtrim(coalesce(trim(rtt.therapy_type_desc), 'X')))
   LEFT OUTER JOIN {{ params.param_cr_core_dataset_name }}.ref_result AS rs ON upper(rtrim(coalesce(trim(stg.complicationoutcome), 'XX'))) = upper(rtrim(coalesce(trim(rs.nav_result_desc), 'XX')))
   WHERE upper(stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_complication.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_complication) ) AS ms ON mt.cn_patient_complication_sid = ms.cn_patient_complication_sid
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.core_record_type_id, 0) = coalesce(ms.core_record_type_id, 0)
     AND coalesce(mt.core_record_type_id, 1) = coalesce(ms.core_record_type_id, 1))
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
AND (coalesce(mt.complication_date, DATE '1970-01-01') = coalesce(ms.complication_date, DATE '1970-01-01')
     AND coalesce(mt.complication_date, DATE '1970-01-02') = coalesce(ms.complication_date, DATE '1970-01-02'))
AND (coalesce(mt.therapy_type_id, 0) = coalesce(ms.therapy_type_id, 0)
     AND coalesce(mt.therapy_type_id, 1) = coalesce(ms.therapy_type_id, 1))
AND (upper(coalesce(mt.treatment_stopped_ind, '0')) = upper(coalesce(ms.treatment_stopped_ind, '0'))
     AND upper(coalesce(mt.treatment_stopped_ind, '1')) = upper(coalesce(ms.treatment_stopped_ind, '1')))
AND (coalesce(mt.outcome_result_id, 0) = coalesce(ms.outcome_result_id, 0)
     AND coalesce(mt.outcome_result_id, 1) = coalesce(ms.outcome_result_id, 1))
AND (upper(coalesce(mt.complication_text, '0')) = upper(coalesce(ms.complication_text, '0'))
     AND upper(coalesce(mt.complication_text, '1')) = upper(coalesce(ms.complication_text, '1')))
AND (upper(coalesce(mt.specific_complication_text, '0')) = upper(coalesce(ms.specific_complication_text, '0'))
     AND upper(coalesce(mt.specific_complication_text, '1')) = upper(coalesce(ms.specific_complication_text, '1')))
AND (upper(coalesce(mt.comment_text, '0')) = upper(coalesce(ms.comment_text, '0'))
     AND upper(coalesce(mt.comment_text, '1')) = upper(coalesce(ms.comment_text, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_complication_sid,
        nav_patient_id,
        core_record_type_id,
        tumor_type_id,
        diagnosis_result_id,
        nav_diagnosis_id,
        navigator_id,
        coid,
        company_code,
        complication_date,
        therapy_type_id,
        treatment_stopped_ind,
        outcome_result_id,
        complication_text,
        specific_complication_text,
        comment_text,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_complication_sid, ms.nav_patient_id, ms.core_record_type_id, ms.tumor_type_id, ms.diagnosis_result_id, ms.nav_diagnosis_id, ms.navigator_id, ms.coid, ms.company_code, ms.complication_date, ms.therapy_type_id, ms.treatment_stopped_ind, ms.outcome_result_id, ms.complication_text, ms.specific_complication_text, ms.comment_text, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_complication_sid
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_complication
      GROUP BY cn_patient_complication_sid
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_complication');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_Patient_Complication');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF