CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_disease_assess_detail
   OPTIONS(description='Contains detail disease assessment data for hematology patient')
  AS SELECT
      cn_patient_heme_disease_assess_detail.cn_patient_heme_sid,
      cn_patient_heme_disease_assess_detail.disease_assess_measure_type_id,
      cn_patient_heme_disease_assess_detail.disease_assess_measure_value_text,
      cn_patient_heme_disease_assess_detail.hashbite_ssk,
      cn_patient_heme_disease_assess_detail.source_system_code,
      cn_patient_heme_disease_assess_detail.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_disease_assess_detail
  ;
