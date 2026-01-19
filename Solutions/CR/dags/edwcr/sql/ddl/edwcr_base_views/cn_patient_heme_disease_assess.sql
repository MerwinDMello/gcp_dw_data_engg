CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_disease_assess
   OPTIONS(description='Contains disease asessment data for Hematology patient')
  AS SELECT
      cn_patient_heme_disease_assess.cn_patient_heme_sid,
      cn_patient_heme_disease_assess.test_type_id,
      cn_patient_heme_disease_assess.sample_type_id,
      cn_patient_heme_disease_assess.disease_assess_source_id,
      cn_patient_heme_disease_assess.disease_assess_facility_id,
      cn_patient_heme_disease_assess.nav_patient_id,
      cn_patient_heme_disease_assess.tumor_type_id,
      cn_patient_heme_disease_assess.diagnosis_result_id,
      cn_patient_heme_disease_assess.nav_diagnosis_id,
      cn_patient_heme_disease_assess.navigator_id,
      cn_patient_heme_disease_assess.coid,
      cn_patient_heme_disease_assess.company_code,
      cn_patient_heme_disease_assess.collection_date,
      cn_patient_heme_disease_assess.disease_status_id,
      cn_patient_heme_disease_assess.treatment_status_id,
      cn_patient_heme_disease_assess.initial_diagnosis_ind,
      cn_patient_heme_disease_assess.disease_monitoring_ind,
      cn_patient_heme_disease_assess.comment_text,
      cn_patient_heme_disease_assess.hashbite_ssk,
      cn_patient_heme_disease_assess.source_system_code,
      cn_patient_heme_disease_assess.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_disease_assess
  ;
