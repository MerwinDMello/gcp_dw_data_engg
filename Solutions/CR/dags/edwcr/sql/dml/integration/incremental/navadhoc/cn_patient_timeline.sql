DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_timeline.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- #####################################################################################
-- #  TARGET TABLE		: EDWCR.CN_PATIENT_TIMELINE	              #
-- #  TARGET  DATABASE	   	: EDWCR	 						#
-- #  SOURCE		   	: EDWCR_staging.CN_PATIENT_TIMELINE_STG		#
-- #	                                                                        	#
-- #  INITIAL RELEASE	   	: 								#
-- #  PROJECT             	: 	 		    					#
-- #  ------------------------------------------------------------------------		#
-- #                                                                              	#
-- #####################################################################################
-- bteq << EOF >> $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_PATIENT_TIMELINE;;
 --' FOR SESSION;;
 /* Collect Stats On Staging table */ -- CALL DBADMIN_PROCS.collect_stats_table('edwcr_staging','CN_PATIENT_TIMELINE_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Delete the records from Core table which don't exist in the Staging table */ BEGIN
SET _ERROR_CODE = 0;


DELETE
FROM {{ params.param_cr_core_dataset_name }}.cn_patient_timeline
WHERE upper(cn_patient_timeline.hashbite_ssk) NOT IN
    (SELECT upper(cn_patient_timeline_stg.hashbite_ssk) AS hashbite_ssk
     FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_timeline_stg);


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Insert the new records into the Core Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cn_patient_timeline AS mt USING
  (SELECT DISTINCT cn_patient_timeline_stg.cn_patient_timeline_id,
                   cn_patient_timeline_stg.nav_patient_id,
                   cn_patient_timeline_stg.tumor_type_id,
                   cn_patient_timeline_stg.navigator_id,
                   cn_patient_timeline_stg.coid AS coid,
                   cn_patient_timeline_stg.company_code AS company_code,
                   cn_patient_timeline_stg.nav_referred_date,
                   cn_patient_timeline_stg.first_treatment_date,
                   cn_patient_timeline_stg.first_consult_date,
                   cn_patient_timeline_stg.first_imaging_date,
                   cn_patient_timeline_stg.first_medical_oncology_date,
                   cn_patient_timeline_stg.first_radiation_oncology_date,
                   cn_patient_timeline_stg.first_diagnosis_date,
                   cn_patient_timeline_stg.first_biopsy_date,
                   cn_patient_timeline_stg.first_surgery_consult_date,
                   cn_patient_timeline_stg.first_surgery_date,
                   cn_patient_timeline_stg.surv_care_plan_close_date,
                   cn_patient_timeline_stg.surv_care_plan_resolve_date,
                   cn_patient_timeline_stg.end_treatment_date,
                   cn_patient_timeline_stg.death_date,
                   cn_patient_timeline_stg.diag_first_trt_day_num,
                   cn_patient_timeline_stg.diag_first_trt_available_ind AS diagnosis_first_treatment_available_ind,
                   cn_patient_timeline_stg.hashbite_ssk,
                   cn_patient_timeline_stg.source_system_code AS source_system_code,
                   datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_timeline_stg
   WHERE upper(cn_patient_timeline_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_timeline.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_timeline) ) AS ms ON mt.cn_patient_timeline_id = ms.cn_patient_timeline_id
AND (coalesce(mt.nav_patient_id, NUMERIC '0') = coalesce(ms.nav_patient_id, NUMERIC '0')
     AND coalesce(mt.nav_patient_id, NUMERIC '1') = coalesce(ms.nav_patient_id, NUMERIC '1'))
AND (coalesce(mt.tumor_type_id, 0) = coalesce(ms.tumor_type_id, 0)
     AND coalesce(mt.tumor_type_id, 1) = coalesce(ms.tumor_type_id, 1))
AND (coalesce(mt.navigator_id, 0) = coalesce(ms.navigator_id, 0)
     AND coalesce(mt.navigator_id, 1) = coalesce(ms.navigator_id, 1))
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.nav_referred_date, DATE '1970-01-01') = coalesce(ms.nav_referred_date, DATE '1970-01-01')
     AND coalesce(mt.nav_referred_date, DATE '1970-01-02') = coalesce(ms.nav_referred_date, DATE '1970-01-02'))
AND (coalesce(mt.first_treatment_date, DATE '1970-01-01') = coalesce(ms.first_treatment_date, DATE '1970-01-01')
     AND coalesce(mt.first_treatment_date, DATE '1970-01-02') = coalesce(ms.first_treatment_date, DATE '1970-01-02'))
AND (coalesce(mt.first_consult_date, DATE '1970-01-01') = coalesce(ms.first_consult_date, DATE '1970-01-01')
     AND coalesce(mt.first_consult_date, DATE '1970-01-02') = coalesce(ms.first_consult_date, DATE '1970-01-02'))
AND (coalesce(mt.first_imaging_date, DATE '1970-01-01') = coalesce(ms.first_imaging_date, DATE '1970-01-01')
     AND coalesce(mt.first_imaging_date, DATE '1970-01-02') = coalesce(ms.first_imaging_date, DATE '1970-01-02'))
AND (coalesce(mt.first_medical_oncology_date, DATE '1970-01-01') = coalesce(ms.first_medical_oncology_date, DATE '1970-01-01')
     AND coalesce(mt.first_medical_oncology_date, DATE '1970-01-02') = coalesce(ms.first_medical_oncology_date, DATE '1970-01-02'))
AND (coalesce(mt.first_radiation_oncology_date, DATE '1970-01-01') = coalesce(ms.first_radiation_oncology_date, DATE '1970-01-01')
     AND coalesce(mt.first_radiation_oncology_date, DATE '1970-01-02') = coalesce(ms.first_radiation_oncology_date, DATE '1970-01-02'))
AND (coalesce(mt.first_diagnosis_date, DATE '1970-01-01') = coalesce(ms.first_diagnosis_date, DATE '1970-01-01')
     AND coalesce(mt.first_diagnosis_date, DATE '1970-01-02') = coalesce(ms.first_diagnosis_date, DATE '1970-01-02'))
AND (coalesce(mt.first_biopsy_date, DATE '1970-01-01') = coalesce(ms.first_biopsy_date, DATE '1970-01-01')
     AND coalesce(mt.first_biopsy_date, DATE '1970-01-02') = coalesce(ms.first_biopsy_date, DATE '1970-01-02'))
AND (coalesce(mt.first_surgery_consult_date, DATE '1970-01-01') = coalesce(ms.first_surgery_consult_date, DATE '1970-01-01')
     AND coalesce(mt.first_surgery_consult_date, DATE '1970-01-02') = coalesce(ms.first_surgery_consult_date, DATE '1970-01-02'))
AND (coalesce(mt.first_surgery_date, DATE '1970-01-01') = coalesce(ms.first_surgery_date, DATE '1970-01-01')
     AND coalesce(mt.first_surgery_date, DATE '1970-01-02') = coalesce(ms.first_surgery_date, DATE '1970-01-02'))
AND (coalesce(mt.survivorship_care_plan_close_date, DATE '1970-01-01') = coalesce(ms.surv_care_plan_close_date, DATE '1970-01-01')
     AND coalesce(mt.survivorship_care_plan_close_date, DATE '1970-01-02') = coalesce(ms.surv_care_plan_close_date, DATE '1970-01-02'))
AND (coalesce(mt.survivorship_care_plan_resolve_date, DATE '1970-01-01') = coalesce(ms.surv_care_plan_resolve_date, DATE '1970-01-01')
     AND coalesce(mt.survivorship_care_plan_resolve_date, DATE '1970-01-02') = coalesce(ms.surv_care_plan_resolve_date, DATE '1970-01-02'))
AND (coalesce(mt.end_treatment_date, DATE '1970-01-01') = coalesce(ms.end_treatment_date, DATE '1970-01-01')
     AND coalesce(mt.end_treatment_date, DATE '1970-01-02') = coalesce(ms.end_treatment_date, DATE '1970-01-02'))
AND (coalesce(mt.death_date, DATE '1970-01-01') = coalesce(ms.death_date, DATE '1970-01-01')
     AND coalesce(mt.death_date, DATE '1970-01-02') = coalesce(ms.death_date, DATE '1970-01-02'))
AND (coalesce(mt.diagnosis_first_treatment_day_num, 0) = coalesce(ms.diag_first_trt_day_num, 0)
     AND coalesce(mt.diagnosis_first_treatment_day_num, 1) = coalesce(ms.diag_first_trt_day_num, 1))
AND (upper(coalesce(mt.diagnosis_first_treatment_available_ind, '0')) = upper(coalesce(ms.diagnosis_first_treatment_available_ind, '0'))
     AND upper(coalesce(mt.diagnosis_first_treatment_available_ind, '1')) = upper(coalesce(ms.diagnosis_first_treatment_available_ind, '1')))
AND (upper(coalesce(mt.hashbite_ssk, '0')) = upper(coalesce(ms.hashbite_ssk, '0'))
     AND upper(coalesce(mt.hashbite_ssk, '1')) = upper(coalesce(ms.hashbite_ssk, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cn_patient_timeline_id,
        nav_patient_id,
        tumor_type_id,
        navigator_id,
        coid,
        company_code,
        nav_referred_date,
        first_treatment_date,
        first_consult_date,
        first_imaging_date,
        first_medical_oncology_date,
        first_radiation_oncology_date,
        first_diagnosis_date,
        first_biopsy_date,
        first_surgery_consult_date,
        first_surgery_date,
        survivorship_care_plan_close_date,
        survivorship_care_plan_resolve_date,
        end_treatment_date,
        death_date,
        diagnosis_first_treatment_day_num,
        diagnosis_first_treatment_available_ind,
        hashbite_ssk,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cn_patient_timeline_id, ms.nav_patient_id, ms.tumor_type_id, ms.navigator_id, ms.coid, ms.company_code, ms.nav_referred_date, ms.first_treatment_date, ms.first_consult_date, ms.first_imaging_date, ms.first_medical_oncology_date, ms.first_radiation_oncology_date, ms.first_diagnosis_date, ms.first_biopsy_date, ms.first_surgery_consult_date, ms.first_surgery_date, ms.surv_care_plan_close_date, ms.surv_care_plan_resolve_date, ms.end_treatment_date, ms.death_date, ms.diag_first_trt_day_num, ms.diagnosis_first_treatment_available_ind, ms.hashbite_ssk, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cn_patient_timeline_id
      FROM {{ params.param_cr_core_dataset_name }}.cn_patient_timeline
      GROUP BY cn_patient_timeline_id
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cn_patient_timeline');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('edwcr','CN_PATIENT_TIMELINE');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF