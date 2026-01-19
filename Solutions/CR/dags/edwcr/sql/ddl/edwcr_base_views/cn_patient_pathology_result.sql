CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_pathology_result
   OPTIONS(description='Contains the results from pathology tests.')
  AS SELECT
      cn_patient_pathology_result.cn_patient_pathology_result_sid,
      cn_patient_pathology_result.pathology_result_type_id,
      cn_patient_pathology_result.core_record_type_id,
      cn_patient_pathology_result.nav_patient_id,
      cn_patient_pathology_result.tumor_type_id,
      cn_patient_pathology_result.diagnosis_result_id,
      cn_patient_pathology_result.nav_diagnosis_id,
      cn_patient_pathology_result.navigator_id,
      cn_patient_pathology_result.coid,
      cn_patient_pathology_result.company_code,
      cn_patient_pathology_result.result_value_text,
      cn_patient_pathology_result.hashbite_ssk,
      cn_patient_pathology_result.source_system_code,
      cn_patient_pathology_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_pathology_result
  ;
