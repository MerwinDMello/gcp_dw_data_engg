-- Translation time: 2024-04-12T11:00:35.054066Z
-- Translation job ID: 8931dd6f-ec0d-4289-86a2-34f1c37489df
-- Source: eim-ops-cs-datamig-dev-0002/cr_bulk_conversion_validation/20240412_0558/input/exp/j_cn_patient_clinical_trial.sql
-- Translated from: Teradata
-- Translated to: BigQuery

SELECT concat(format('%20d', count(*)), ',\t') AS source_string
FROM
  (SELECT cn_patient_clinical_trial_stg.cn_patient_clinical_trial_sid,
          cn_patient_clinical_trial_stg.nav_patient_id,
          cn_patient_clinical_trial_stg.tumor_type_id,
          cn_patient_clinical_trial_stg.diagnosis_result_id,
          cn_patient_clinical_trial_stg.nav_diagnosis_id,
          cn_patient_clinical_trial_stg.navigator_id,
          cn_patient_clinical_trial_stg.coid,
          cn_patient_clinical_trial_stg.company_code,
          cn_patient_clinical_trial_stg.clinical_trial_name,
          cn_patient_clinical_trial_stg.clinical_trial_enrolled_ind,
          cn_patient_clinical_trial_stg.clinical_trial_enrolled_date,
          cn_patient_clinical_trial_stg.clinical_trial_offered_ind,
          cn_patient_clinical_trial_stg.clinical_trial_offered_date,
          cn_patient_clinical_trial_stg.hashbite_ssk,
          cn_patient_clinical_trial_stg.source_system_code,
          datetime_trunc(current_datetime('US/Central'), SECOND) AS dw_last_update_date_time
   FROM {{ params.param_cr_stage_dataset_name }}.cn_patient_clinical_trial_stg
   WHERE upper(cn_patient_clinical_trial_stg.hashbite_ssk) NOT IN
       (SELECT upper(cn_patient_clinical_trial.hashbite_ssk) AS hashbite_ssk
        FROM {{ params.param_cr_core_dataset_name }}.cn_patient_clinical_trial
        FOR SYSTEM_TIME AS OF TIMESTAMP(tableload_start_time, 'US/Central')) ) AS a