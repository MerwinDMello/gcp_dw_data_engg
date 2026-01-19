DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_outcome_timeliness_stg.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ####################################################################################
-- #  TARGET TABLE		: EDWCR_STAGING.Cancer_Patient_Outcome_Timeliness_STG      #
-- #  TARGET  DATABASE	   	: EDWCR_STAGING	 				   #
-- #  SOURCE		   	: 		                                   #
-- #	                                                                           #
-- #  INITIAL RELEASE	   	: 						   #
-- #  PROJECT             	: 	                                                   #
-- #  Created by			:       Bhagyashree Kademani    		   #
-- #  ------------------------------------------------------------------------	   #
-- #                                                                                  #
-- ####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Outcome_Timeliness_STG;;
 --' FOR SESSION;;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate Staging table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_stage_dataset_name }}.cancer_patient_outcome_timeliness_stg;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate STG Table */ BEGIN
SET _ERROR_CODE = 0;


INSERT INTO {{ params.param_cr_stage_dataset_name }}.cancer_patient_outcome_timeliness_stg (cancer_patient_tumor_driver_sk, cancer_patient_driver_sk, cancer_tumor_driver_sk, coid, company_code, length_to_chemo_day_num, length_to_hormone_day_num, length_to_immuno_day_num, length_to_surgery_day_num, length_to_radiation_day_num, length_to_transplant_day_num, length_to_first_treatment_day_num, first_surgery_chemo_elapsed_day_num, first_chemo_surgery_elapsed_day_num, radiation_elapsed_day_num, length_to_surgery_last_contact_day_num, length_to_diagnosis_last_contact_day_num, length_to_diagnosis_last_contact_mth_num, last_contact_date, admission_date, rln10_concordant_ind, rln12_concordant_ind, act_concordant_ind, bcs_concordant_ind, bcsrt_concordant_ind, cbrrt_concordant_ind, cerct_concrodant_ind, cerrt_concrodant_ind, endctrt_concordant_ind, endlrc_concordant_ind, g15rlnc_concordant_ind, lct_concordant_ind, ht_concordant_ind, lnosurg_concordant_ind, mac_concordant_ind, mastrt_concordant_ind, nb_concordant_ind, ovsal_concordant_ind, recrtct_concordance_ind, source_system_code, dw_last_update_date_time)
SELECT cptd.cancer_patient_tumor_driver_sk,
       cptd.cancer_patient_driver_sk,
       cptd.cancer_tumor_driver_sk,
       cptd.coid,
       cptd.company_code,
       cpt.length_to_chemo_day_num,
       cpt.length_to_hormone_day_num,
       cpt.length_to_immuno_day_num,
       cpt.length_to_surgery_day_num,
       cpt.length_to_radiation_day_num,
       cpt.length_to_transplant_day_num,
       cpt.length_to_first_treatment_day_num,
       cpt.first_surgery_chemo_elapsed_day_num,
       cpt.first_chemo_surgery_elapsed_day_num,
       cpt.radiation_elapsed_day_num,
       date_diff(crp.last_contact_date, cpt.first_surgery_date, DAY) AS length_to_surgery_last_contact_day_num,
       date_diff(current_date('US/Central'), cpdd.diagnosis_date, DAY) AS length_to_diagnosis_last_contact_day_num,
       CASE
           WHEN cpdd.diagnosis_date > DATE '1800-01-01' THEN date_diff(current_date('US/Central'), cpdd.diagnosis_date, MONTH)
           ELSE CAST(NULL AS INT64)
       END AS length_to_diagnosis_last_contact_mth_num,
       crp.last_contact_date,
       cpt.admission_date,
       rln10.rln10_concordant_ind,
       rln12.rln12_concordant_ind,
       act.act_concordant_ind,
       bcs.bcs_concordant_ind,
       bcsrt.bcsrt_concordant_ind,
       cbrrt.cbrrt_concordant_ind,
       cerct.cerct_concrodant_ind,
       cerrt.cerrt_concrodant_ind,
       endctrt.endctrt_concordant_ind,
       endlrc.endlrc_concordant_ind,
       g15rln.g15rln_concordant_ind,
       lct.lct_concordant_ind,
       ht.ht_concordant_ind,
       lnosurg.lnosurg_concordant_ind,
       mac.mac_concordant_ind,
       mastrt.mastrt_concordant_ind,
       nb.nb_concordant_ind,
       ovsal.ovsal_concordant_ind,
       recrtct.recrtct_concordant_ind,
       cpt.source_system_code,
       datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
FROM {{ params.param_cr_base_views_dataset_name }}.cr_patient_tumor AS cpt
INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient AS crp ON cpt.cr_patient_id = crp.cr_patient_id
INNER JOIN {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver AS cptd ON cptd.cr_patient_id = cpt.cr_patient_id
AND cptd.cr_tumor_primary_site_id = cpt.tumor_primary_site_id
LEFT OUTER JOIN {{ params.param_cr_base_views_dataset_name }}.cr_patient_diagnosis_detail AS cpdd ON cpt.tumor_id = cpdd.tumor_id
AND cpt.cr_patient_id = cpdd.cr_patient_id
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.rln10_concordant_ind AS rln10 ON cpt.cr_patient_id = rln10.patientid
AND cpt.tumor_id = rln10.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.rln12_concordant_ind AS rln12 ON cpt.cr_patient_id = rln12.patientid
AND cpt.tumor_id = rln12.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.act_concordant_ind AS act ON cpt.cr_patient_id = act.patientid
AND cpt.tumor_id = act.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.bcs_concordant_ind AS bcs ON cpt.cr_patient_id = bcs.patientid
AND cpt.tumor_id = bcs.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.bcsrt_concordant_ind AS bcsrt ON cpt.cr_patient_id = bcsrt.patientid
AND cpt.tumor_id = bcsrt.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.cbrrt_concordant_ind AS cbrrt ON cpt.cr_patient_id = cbrrt.patientid
AND cpt.tumor_id = cbrrt.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.cerct_concrodant_ind AS cerct ON cpt.cr_patient_id = cerct.patientid
AND cpt.tumor_id = cerct.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.cerrt_concrodant_ind AS cerrt ON cpt.cr_patient_id = cerrt.patientid
AND cpt.tumor_id = cerrt.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.endctrt_concordant_ind AS endctrt ON cpt.cr_patient_id = endctrt.patientid
AND cpt.tumor_id = endctrt.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.endlrc_concordant_ind AS endlrc ON cpt.cr_patient_id = endlrc.patientid
AND cpt.tumor_id = endlrc.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.g15rln_concordant_ind AS g15rln ON cpt.cr_patient_id = g15rln.patientid
AND cpt.tumor_id = g15rln.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.lct_concordant_ind AS lct ON cpt.cr_patient_id = lct.patientid
AND cpt.tumor_id = lct.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.ht_concordant_ind AS ht ON cpt.cr_patient_id = ht.patientid
AND cpt.tumor_id = ht.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.lnosurg_concordant_ind AS lnosurg ON cpt.cr_patient_id = lnosurg.patientid
AND cpt.tumor_id = lnosurg.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.mac_concordant_ind AS mac ON cpt.cr_patient_id = mac.patientid
AND cpt.tumor_id = mac.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.mastrt_concordant_ind AS mastrt ON cpt.cr_patient_id = mastrt.patientid
AND cpt.tumor_id = mastrt.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.nb_concordant_ind AS nb ON cpt.cr_patient_id = nb.patientid
AND cpt.tumor_id = nb.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.ovsal_concordant_ind AS ovsal ON cpt.cr_patient_id = ovsal.patientid
AND cpt.tumor_id = ovsal.tumorid
LEFT OUTER JOIN {{ params.param_cr_stage_dataset_name }}.recrtct_concordant_ind AS recrtct ON cpt.cr_patient_id = recrtct.patientid
AND cpt.tumor_id = recrtct.tumorid QUALIFY row_number() OVER (PARTITION BY cpt.cr_patient_id,
                                                                           cpt.tumor_primary_site_id
                                                              ORDER BY cpt.tumor_id DESC) = 1;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR_STAGING','Cancer_Patient_Outcome_Timeliness_STG');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF