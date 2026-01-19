CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_diagnosis
   OPTIONS(description='Contains diagnosis details for Hematology patient')
  AS SELECT
      cn_patient_heme_diagnosis.cn_patient_heme_diagnosis_sid,
      cn_patient_heme_diagnosis.nav_patient_id,
      cn_patient_heme_diagnosis.disease_status_id,
      cn_patient_heme_diagnosis.tumor_type_id,
      cn_patient_heme_diagnosis.diagnosis_result_id,
      cn_patient_heme_diagnosis.nav_diagnosis_id,
      cn_patient_heme_diagnosis.navigator_id,
      cn_patient_heme_diagnosis.coid,
      cn_patient_heme_diagnosis.company_code,
      cn_patient_heme_diagnosis.speciman_date,
      cn_patient_heme_diagnosis.disease_diagnosis_text,
      cn_patient_heme_diagnosis.therapy_related_ind,
      cn_patient_heme_diagnosis.transformed_from_mds_ind,
      cn_patient_heme_diagnosis.mipi_text,
      cn_patient_heme_diagnosis.ipi_text,
      cn_patient_heme_diagnosis.flipi_text,
      cn_patient_heme_diagnosis.aids_related_ind,
      cn_patient_heme_diagnosis.comment_text,
      cn_patient_heme_diagnosis.classification_text,
      cn_patient_heme_diagnosis.sub_classification_text,
      cn_patient_heme_diagnosis.nhl_type_text,
      cn_patient_heme_diagnosis.other_nhl_type_text,
      cn_patient_heme_diagnosis.transformed_disease_text,
      cn_patient_heme_diagnosis.non_malignant_type_text,
      cn_patient_heme_diagnosis.feature_text,
      cn_patient_heme_diagnosis.risk_category_text,
      cn_patient_heme_diagnosis.mds_disease_risk_text,
      cn_patient_heme_diagnosis.staging_field_1_text,
      cn_patient_heme_diagnosis.staging_field_2_text,
      cn_patient_heme_diagnosis.hashbite_ssk,
      cn_patient_heme_diagnosis.source_system_code,
      cn_patient_heme_diagnosis.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_diagnosis
  ;
