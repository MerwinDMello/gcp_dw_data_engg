CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_lab_result
   OPTIONS(description='Contains the lab results for each patient.')
  AS SELECT
      cn_patient_lab_result.nav_patient_lab_result_sid,
      cn_patient_lab_result.lab_type_id,
      cn_patient_lab_result.core_record_type_id,
      cn_patient_lab_result.nav_patient_id,
      cn_patient_lab_result.tumor_type_id,
      cn_patient_lab_result.diagnosis_result_id,
      cn_patient_lab_result.nav_diagnosis_id,
      cn_patient_lab_result.navigator_id,
      cn_patient_lab_result.coid,
      cn_patient_lab_result.company_code,
      cn_patient_lab_result.lab_date,
      cn_patient_lab_result.lab_result_text,
      cn_patient_lab_result.comment_text,
      cn_patient_lab_result.hashbite_ssk,
      cn_patient_lab_result.source_system_code,
      cn_patient_lab_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_lab_result
  ;
