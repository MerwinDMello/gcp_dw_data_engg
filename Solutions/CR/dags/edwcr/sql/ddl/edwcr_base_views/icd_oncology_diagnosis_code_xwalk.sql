CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.icd_oncology_diagnosis_code_xwalk
   OPTIONS(description='Crosswalks ICD Oncology Codes to ICD10 Diagnosis Codes.')
  AS SELECT
      icd_oncology_diagnosis_code_xwalk.icd_oncology_code,
      icd_oncology_diagnosis_code_xwalk.icd_oncology_type_code,
      icd_oncology_diagnosis_code_xwalk.icd_oncology_category_type_code,
      icd_oncology_diagnosis_code_xwalk.diagnosis_code,
      icd_oncology_diagnosis_code_xwalk.diagnosis_type_code,
      icd_oncology_diagnosis_code_xwalk.source_system_code,
      icd_oncology_diagnosis_code_xwalk.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.icd_oncology_diagnosis_code_xwalk
  ;
