CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme
   OPTIONS(description='Contains details for Hematology patient')
  AS SELECT
      cn_patient_heme.cn_patient_heme_sid,
      cn_patient_heme.nav_patient_id,
      cn_patient_heme.hematologist_physician_id,
      cn_patient_heme.tumor_type_id,
      cn_patient_heme.diagnosis_result_id,
      cn_patient_heme.nav_diagnosis_id,
      cn_patient_heme.navigator_id,
      cn_patient_heme.coid,
      cn_patient_heme.company_code,
      cn_patient_heme.transportation_text,
      cn_patient_heme.drug_use_history_text,
      cn_patient_heme.hashbite_ssk,
      cn_patient_heme.source_system_code,
      cn_patient_heme.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme
  ;
