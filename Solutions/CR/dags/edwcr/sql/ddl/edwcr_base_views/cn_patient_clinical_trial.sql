CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_clinical_trial
   OPTIONS(description='Contains the details associated with the clinical trial a patient was enrolled in.')
  AS SELECT
      cn_patient_clinical_trial.cn_patient_clinical_trial_sid,
      cn_patient_clinical_trial.nav_patient_id,
      cn_patient_clinical_trial.tumor_type_id,
      cn_patient_clinical_trial.diagnosis_result_id,
      cn_patient_clinical_trial.nav_diagnosis_id,
      cn_patient_clinical_trial.navigator_id,
      cn_patient_clinical_trial.coid,
      cn_patient_clinical_trial.company_code,
      cn_patient_clinical_trial.clinical_trial_name,
      cn_patient_clinical_trial.clinical_trial_enrolled_ind,
      cn_patient_clinical_trial.clinical_trial_enrolled_date,
      cn_patient_clinical_trial.clinical_trial_offered_ind,
      cn_patient_clinical_trial.clinical_trial_offered_date,
      cn_patient_clinical_trial.hashbite_ssk,
      cn_patient_clinical_trial.source_system_code,
      cn_patient_clinical_trial.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_clinical_trial
  ;
