CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_cancer_diagnosis
   OPTIONS(description='Table contains all ICD9 diagnosis codes related to cancer and the description to each.')
  AS SELECT
      ref_cancer_diagnosis.diagnosis_code,
      ref_cancer_diagnosis.diagnosis_type_code,
      ref_cancer_diagnosis.diagnosis_desc,
      ref_cancer_diagnosis.diagnosis_formatted_code,
      ref_cancer_diagnosis.comorbidity_sw,
      ref_cancer_diagnosis.eff_from_date,
      ref_cancer_diagnosis.eff_to_date,
      ref_cancer_diagnosis.source_system_code,
      ref_cancer_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_cancer_diagnosis
  ;
