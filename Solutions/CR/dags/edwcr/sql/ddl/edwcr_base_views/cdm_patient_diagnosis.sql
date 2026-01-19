CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cdm_patient_diagnosis
   OPTIONS(description='Contains all diagnoses at the encounter level.')
  AS SELECT
      cdm_patient_diagnosis.patient_dw_id,
      cdm_patient_diagnosis.diagnosis_cycle_code,
      cdm_patient_diagnosis.diagnosis_mapped_code,
      cdm_patient_diagnosis.diagnosis_code,
      cdm_patient_diagnosis.diagnosis_type_code,
      cdm_patient_diagnosis.diagnosis_type_code_desc,
      cdm_patient_diagnosis.coid,
      cdm_patient_diagnosis.company_code,
      cdm_patient_diagnosis.pat_acct_num,
      cdm_patient_diagnosis.diagnosis_rank_num,
      cdm_patient_diagnosis.diagnosis_short_desc,
      cdm_patient_diagnosis.cancer_diagnosis_ind,
      cdm_patient_diagnosis.source_system_code,
      cdm_patient_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cdm_patient_diagnosis
  ;
