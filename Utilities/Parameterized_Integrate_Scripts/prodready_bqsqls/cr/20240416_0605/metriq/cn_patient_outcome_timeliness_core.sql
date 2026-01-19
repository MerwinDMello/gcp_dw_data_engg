DECLARE DUP_COUNT INT64;

-- Translation time: 2024-03-26T19:50:24.234396Z
-- Translation job ID: 2a08e83e-0c7a-4cb8-a60b-a76878d97341
-- Source: eim-ops-cs-datamig-dev-0002/cr_conversion/9fg4Bx/input/cn_patient_outcome_timeliness_core.sql
-- Translated from: bteq
-- Translated to: BigQuery
 DECLARE _ERROR_CODE INT64;

DECLARE _ERROR_MSG STRING DEFAULT '';

---- ####################################################################################
-- #  TARGET TABLE		: EDWCR.Cancer_Patient_Outcome_Timeliness                  #
-- #  TARGET  DATABASE	: EDWCR	 				           #
-- #  SOURCE		: EDWCR_STAGING.Cancer_Patient_Outcome_Timeliness_STG	   #
-- #	                                                                           #
-- #  INITIAL RELEASE	: 						           #
-- #  PROJECT             	: 	                                                   #
-- #  Created by           :       Bhagyashree Kademani    		           #
-- #  ------------------------------------------------------------------------	   #
-- #                                                                                  #
-- ####################################################################################
-- bteq << EOF > $1;;
 -- SET QUERY_BAND = 'App=EDWCR_ETL;
 --Job=J_CN_Patient_Outcome_Timeliness_CORE;;
 --' FOR SESSION;;
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Truncate CORE table */ BEGIN
SET _ERROR_CODE = 0;

TRUNCATE TABLE {{ params.param_cr_core_dataset_name }}.cancer_patient_outcome_timeliness;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

/* Populate CORE  Table */ BEGIN
SET _ERROR_CODE = 0;

BEGIN TRANSACTION;


MERGE INTO {{ params.param_cr_core_dataset_name }}.cancer_patient_outcome_timeliness AS mt USING
  (SELECT DISTINCT stg.cancer_patient_tumor_driver_sk,
                   stg.cancer_patient_driver_sk,
                   stg.cancer_tumor_driver_sk,
                   stg.coid AS coid,
                   stg.company_code AS company_code,
                   stg.length_to_chemo_day_num,
                   stg.length_to_hormone_day_num,
                   stg.length_to_immuno_day_num,
                   stg.length_to_surgery_day_num,
                   stg.length_to_radiation_day_num,
                   stg.length_to_transplant_day_num,
                   stg.length_to_first_treatment_day_num,
                   stg.first_surgery_chemo_elapsed_day_num,
                   stg.first_chemo_surgery_elapsed_day_num,
                   stg.radiation_elapsed_day_num,
                   stg.length_to_surgery_last_contact_day_num,
                   stg.length_to_diagnosis_last_contact_day_num,
                   stg.length_to_diagnosis_last_contact_mth_num,
                   stg.last_contact_date,
                   stg.admission_date,
                   stg.rln10_concordant_ind AS rln10_concordant_ind,
                   stg.rln12_concordant_ind AS rln12_concordant_ind,
                   stg.act_concordant_ind AS act_concordant_ind,
                   stg.bcs_concordant_ind AS bcs_concordant_ind,
                   stg.bcsrt_concordant_ind AS bcsrt_concordant_ind,
                   stg.cbrrt_concordant_ind AS cbrrt_concordant_ind,
                   stg.cerct_concrodant_ind AS cerct_concrodant_ind,
                   stg.cerrt_concrodant_ind AS cerrt_concrodant_ind,
                   stg.endctrt_concordant_ind AS endctrt_concordant_ind,
                   stg.endlrc_concordant_ind AS endlrc_concordant_ind,
                   stg.g15rlnc_concordant_ind AS g15rlnc_concordant_ind,
                   stg.lct_concordant_ind AS lct_concordant_ind,
                   stg.ht_concordant_ind AS ht_concordant_ind,
                   stg.lnosurg_concordant_ind AS lnosurg_concordant_ind,
                   stg.mac_concordant_ind AS mac_concordant_ind,
                   stg.mastrt_concordant_ind AS mastrt_concordant_ind,
                   stg.nb_concordant_ind AS nb_concordant_ind,
                   stg.ovsal_concordant_ind AS ovsal_concordant_ind,
                   stg.recrtct_concordance_ind AS recrtct_concordance_ind,
                   stg.source_system_code AS source_system_code,
                   stg.dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cancer_patient_outcome_timeliness_stg AS stg) AS ms ON mt.cancer_patient_tumor_driver_sk = ms.cancer_patient_tumor_driver_sk
AND mt.cancer_patient_driver_sk = ms.cancer_patient_driver_sk
AND mt.cancer_tumor_driver_sk = ms.cancer_tumor_driver_sk
AND mt.coid = ms.coid
AND mt.company_code = ms.company_code
AND (coalesce(mt.length_to_chemo_day_num, 0) = coalesce(ms.length_to_chemo_day_num, 0)
     AND coalesce(mt.length_to_chemo_day_num, 1) = coalesce(ms.length_to_chemo_day_num, 1))
AND (coalesce(mt.length_to_hormone_day_num, 0) = coalesce(ms.length_to_hormone_day_num, 0)
     AND coalesce(mt.length_to_hormone_day_num, 1) = coalesce(ms.length_to_hormone_day_num, 1))
AND (coalesce(mt.length_to_immuno_day_num, 0) = coalesce(ms.length_to_immuno_day_num, 0)
     AND coalesce(mt.length_to_immuno_day_num, 1) = coalesce(ms.length_to_immuno_day_num, 1))
AND (coalesce(mt.length_to_surgery_day_num, 0) = coalesce(ms.length_to_surgery_day_num, 0)
     AND coalesce(mt.length_to_surgery_day_num, 1) = coalesce(ms.length_to_surgery_day_num, 1))
AND (coalesce(mt.length_to_radiation_day_num, 0) = coalesce(ms.length_to_radiation_day_num, 0)
     AND coalesce(mt.length_to_radiation_day_num, 1) = coalesce(ms.length_to_radiation_day_num, 1))
AND (coalesce(mt.length_to_transplant_day_num, 0) = coalesce(ms.length_to_transplant_day_num, 0)
     AND coalesce(mt.length_to_transplant_day_num, 1) = coalesce(ms.length_to_transplant_day_num, 1))
AND (coalesce(mt.length_to_first_treatment_day_num, 0) = coalesce(ms.length_to_first_treatment_day_num, 0)
     AND coalesce(mt.length_to_first_treatment_day_num, 1) = coalesce(ms.length_to_first_treatment_day_num, 1))
AND (coalesce(mt.first_surgery_chemo_elapsed_day_num, 0) = coalesce(ms.first_surgery_chemo_elapsed_day_num, 0)
     AND coalesce(mt.first_surgery_chemo_elapsed_day_num, 1) = coalesce(ms.first_surgery_chemo_elapsed_day_num, 1))
AND (coalesce(mt.first_chemo_surgery_elapsed_day_num, 0) = coalesce(ms.first_chemo_surgery_elapsed_day_num, 0)
     AND coalesce(mt.first_chemo_surgery_elapsed_day_num, 1) = coalesce(ms.first_chemo_surgery_elapsed_day_num, 1))
AND (coalesce(mt.radiation_elapsed_day_num, 0) = coalesce(ms.radiation_elapsed_day_num, 0)
     AND coalesce(mt.radiation_elapsed_day_num, 1) = coalesce(ms.radiation_elapsed_day_num, 1))
AND (coalesce(mt.length_to_surgery_last_contact_day_num, 0) = coalesce(ms.length_to_surgery_last_contact_day_num, 0)
     AND coalesce(mt.length_to_surgery_last_contact_day_num, 1) = coalesce(ms.length_to_surgery_last_contact_day_num, 1))
AND (coalesce(mt.length_to_diagnosis_last_contact_day_num, 0) = coalesce(ms.length_to_diagnosis_last_contact_day_num, 0)
     AND coalesce(mt.length_to_diagnosis_last_contact_day_num, 1) = coalesce(ms.length_to_diagnosis_last_contact_day_num, 1))
AND (coalesce(mt.length_to_diagnosis_last_contact_mth_num, 0) = coalesce(ms.length_to_diagnosis_last_contact_mth_num, 0)
     AND coalesce(mt.length_to_diagnosis_last_contact_mth_num, 1) = coalesce(ms.length_to_diagnosis_last_contact_mth_num, 1))
AND (coalesce(mt.last_contact_date, DATE '1970-01-01') = coalesce(ms.last_contact_date, DATE '1970-01-01')
     AND coalesce(mt.last_contact_date, DATE '1970-01-02') = coalesce(ms.last_contact_date, DATE '1970-01-02'))
AND (coalesce(mt.admission_date, DATE '1970-01-01') = coalesce(ms.admission_date, DATE '1970-01-01')
     AND coalesce(mt.admission_date, DATE '1970-01-02') = coalesce(ms.admission_date, DATE '1970-01-02'))
AND (upper(coalesce(mt.rln10_concordant_ind, '0')) = upper(coalesce(ms.rln10_concordant_ind, '0'))
     AND upper(coalesce(mt.rln10_concordant_ind, '1')) = upper(coalesce(ms.rln10_concordant_ind, '1')))
AND (upper(coalesce(mt.rln12_concordant_ind, '0')) = upper(coalesce(ms.rln12_concordant_ind, '0'))
     AND upper(coalesce(mt.rln12_concordant_ind, '1')) = upper(coalesce(ms.rln12_concordant_ind, '1')))
AND (upper(coalesce(mt.act_concordant_ind, '0')) = upper(coalesce(ms.act_concordant_ind, '0'))
     AND upper(coalesce(mt.act_concordant_ind, '1')) = upper(coalesce(ms.act_concordant_ind, '1')))
AND (upper(coalesce(mt.bcs_concordant_ind, '0')) = upper(coalesce(ms.bcs_concordant_ind, '0'))
     AND upper(coalesce(mt.bcs_concordant_ind, '1')) = upper(coalesce(ms.bcs_concordant_ind, '1')))
AND (upper(coalesce(mt.bcsrt_concordant_ind, '0')) = upper(coalesce(ms.bcsrt_concordant_ind, '0'))
     AND upper(coalesce(mt.bcsrt_concordant_ind, '1')) = upper(coalesce(ms.bcsrt_concordant_ind, '1')))
AND (upper(coalesce(mt.cbrrt_concordant_ind, '0')) = upper(coalesce(ms.cbrrt_concordant_ind, '0'))
     AND upper(coalesce(mt.cbrrt_concordant_ind, '1')) = upper(coalesce(ms.cbrrt_concordant_ind, '1')))
AND (upper(coalesce(mt.cerct_concrodant_ind, '0')) = upper(coalesce(ms.cerct_concrodant_ind, '0'))
     AND upper(coalesce(mt.cerct_concrodant_ind, '1')) = upper(coalesce(ms.cerct_concrodant_ind, '1')))
AND (upper(coalesce(mt.cerrt_concrodant_ind, '0')) = upper(coalesce(ms.cerrt_concrodant_ind, '0'))
     AND upper(coalesce(mt.cerrt_concrodant_ind, '1')) = upper(coalesce(ms.cerrt_concrodant_ind, '1')))
AND (upper(coalesce(mt.endctrt_concordant_ind, '0')) = upper(coalesce(ms.endctrt_concordant_ind, '0'))
     AND upper(coalesce(mt.endctrt_concordant_ind, '1')) = upper(coalesce(ms.endctrt_concordant_ind, '1')))
AND (upper(coalesce(mt.endlrc_concordant_ind, '0')) = upper(coalesce(ms.endlrc_concordant_ind, '0'))
     AND upper(coalesce(mt.endlrc_concordant_ind, '1')) = upper(coalesce(ms.endlrc_concordant_ind, '1')))
AND (upper(coalesce(mt.g15rlnc_concordant_ind, '0')) = upper(coalesce(ms.g15rlnc_concordant_ind, '0'))
     AND upper(coalesce(mt.g15rlnc_concordant_ind, '1')) = upper(coalesce(ms.g15rlnc_concordant_ind, '1')))
AND (upper(coalesce(mt.lct_concordant_ind, '0')) = upper(coalesce(ms.lct_concordant_ind, '0'))
     AND upper(coalesce(mt.lct_concordant_ind, '1')) = upper(coalesce(ms.lct_concordant_ind, '1')))
AND (upper(coalesce(mt.ht_concordant_ind, '0')) = upper(coalesce(ms.ht_concordant_ind, '0'))
     AND upper(coalesce(mt.ht_concordant_ind, '1')) = upper(coalesce(ms.ht_concordant_ind, '1')))
AND (upper(coalesce(mt.lnosurg_concordant_ind, '0')) = upper(coalesce(ms.lnosurg_concordant_ind, '0'))
     AND upper(coalesce(mt.lnosurg_concordant_ind, '1')) = upper(coalesce(ms.lnosurg_concordant_ind, '1')))
AND (upper(coalesce(mt.mac_concordant_ind, '0')) = upper(coalesce(ms.mac_concordant_ind, '0'))
     AND upper(coalesce(mt.mac_concordant_ind, '1')) = upper(coalesce(ms.mac_concordant_ind, '1')))
AND (upper(coalesce(mt.mastrt_concordant_ind, '0')) = upper(coalesce(ms.mastrt_concordant_ind, '0'))
     AND upper(coalesce(mt.mastrt_concordant_ind, '1')) = upper(coalesce(ms.mastrt_concordant_ind, '1')))
AND (upper(coalesce(mt.nb_concordant_ind, '0')) = upper(coalesce(ms.nb_concordant_ind, '0'))
     AND upper(coalesce(mt.nb_concordant_ind, '1')) = upper(coalesce(ms.nb_concordant_ind, '1')))
AND (upper(coalesce(mt.ovsal_concordant_ind, '0')) = upper(coalesce(ms.ovsal_concordant_ind, '0'))
     AND upper(coalesce(mt.ovsal_concordant_ind, '1')) = upper(coalesce(ms.ovsal_concordant_ind, '1')))
AND (upper(coalesce(mt.recrtct_concordance_ind, '0')) = upper(coalesce(ms.recrtct_concordance_ind, '0'))
     AND upper(coalesce(mt.recrtct_concordance_ind, '1')) = upper(coalesce(ms.recrtct_concordance_ind, '1')))
AND mt.source_system_code = ms.source_system_code
AND mt.dw_last_update_date_time = ms.dw_last_update_date_time WHEN NOT MATCHED BY TARGET THEN
INSERT (cancer_patient_tumor_driver_sk,
        cancer_patient_driver_sk,
        cancer_tumor_driver_sk,
        coid,
        company_code,
        length_to_chemo_day_num,
        length_to_hormone_day_num,
        length_to_immuno_day_num,
        length_to_surgery_day_num,
        length_to_radiation_day_num,
        length_to_transplant_day_num,
        length_to_first_treatment_day_num,
        first_surgery_chemo_elapsed_day_num,
        first_chemo_surgery_elapsed_day_num,
        radiation_elapsed_day_num,
        length_to_surgery_last_contact_day_num,
        length_to_diagnosis_last_contact_day_num,
        length_to_diagnosis_last_contact_mth_num,
        last_contact_date,
        admission_date,
        rln10_concordant_ind,
        rln12_concordant_ind,
        act_concordant_ind,
        bcs_concordant_ind,
        bcsrt_concordant_ind,
        cbrrt_concordant_ind,
        cerct_concrodant_ind,
        cerrt_concrodant_ind,
        endctrt_concordant_ind,
        endlrc_concordant_ind,
        g15rlnc_concordant_ind,
        lct_concordant_ind,
        ht_concordant_ind,
        lnosurg_concordant_ind,
        mac_concordant_ind,
        mastrt_concordant_ind,
        nb_concordant_ind,
        ovsal_concordant_ind,
        recrtct_concordance_ind,
        source_system_code,
        dw_last_update_date_time)
VALUES (ms.cancer_patient_tumor_driver_sk, ms.cancer_patient_driver_sk, ms.cancer_tumor_driver_sk, ms.coid, ms.company_code, ms.length_to_chemo_day_num, ms.length_to_hormone_day_num, ms.length_to_immuno_day_num, ms.length_to_surgery_day_num, ms.length_to_radiation_day_num, ms.length_to_transplant_day_num, ms.length_to_first_treatment_day_num, ms.first_surgery_chemo_elapsed_day_num, ms.first_chemo_surgery_elapsed_day_num, ms.radiation_elapsed_day_num, ms.length_to_surgery_last_contact_day_num, ms.length_to_diagnosis_last_contact_day_num, ms.length_to_diagnosis_last_contact_mth_num, ms.last_contact_date, ms.admission_date, ms.rln10_concordant_ind, ms.rln12_concordant_ind, ms.act_concordant_ind, ms.bcs_concordant_ind, ms.bcsrt_concordant_ind, ms.cbrrt_concordant_ind, ms.cerct_concrodant_ind, ms.cerrt_concrodant_ind, ms.endctrt_concordant_ind, ms.endlrc_concordant_ind, ms.g15rlnc_concordant_ind, ms.lct_concordant_ind, ms.ht_concordant_ind, ms.lnosurg_concordant_ind, ms.mac_concordant_ind, ms.mastrt_concordant_ind, ms.nb_concordant_ind, ms.ovsal_concordant_ind, ms.recrtct_concordance_ind, ms.source_system_code, ms.dw_last_update_date_time);


SET DUP_COUNT =
  (SELECT count(*)
   FROM
     (SELECT cancer_patient_tumor_driver_sk
      FROM {{ params.param_cr_core_dataset_name }}.cancer_patient_outcome_timeliness
      GROUP BY cancer_patient_tumor_driver_sk
      HAVING count(*) > 1));

IF DUP_COUNT <> 0 THEN
ROLLBACK TRANSACTION;

RAISE USING MESSAGE = concat('Duplicates are not allowed in the table {{ params.param_cr_core_dataset_name }}.cancer_patient_outcome_timeliness');

ELSE
COMMIT TRANSACTION;

END IF;


EXCEPTION WHEN ERROR THEN
SET _ERROR_CODE = 1;


SET _ERROR_MSG = @@error.message;

END;

IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

-- CALL DBADMIN_PROCS.collect_stats_table('EDWCR','Cancer_Patient_Outcome_Timeliness');
 IF _ERROR_CODE <> 0 THEN RAISE USING MESSAGE = concat('Script terminated with return code: ', _ERROR_CODE, ' ', _ERROR_MSG);

END IF;

---- EOF