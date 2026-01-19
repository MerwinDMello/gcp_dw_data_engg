CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_core_adherence
   OPTIONS(description='Contains details associated with the patients treatments. Acts as a guide to see if a patient has been through certain thresholds of navigation.')
  AS SELECT
      cn_patient_core_adherence.cn_patient_core_adherence_sid,
      cn_patient_core_adherence.core_adherence_measure_id,
      cn_patient_core_adherence.nav_patient_id,
      cn_patient_core_adherence.tumor_type_id,
      cn_patient_core_adherence.diagnosis_result_id,
      cn_patient_core_adherence.nav_diagnosis_id,
      cn_patient_core_adherence.navigator_id,
      cn_patient_core_adherence.coid,
      cn_patient_core_adherence.company_code,
      cn_patient_core_adherence.core_adherence_measure_text,
      cn_patient_core_adherence.hashbite_ssk,
      cn_patient_core_adherence.source_system_code,
      cn_patient_core_adherence.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_core_adherence
  ;
