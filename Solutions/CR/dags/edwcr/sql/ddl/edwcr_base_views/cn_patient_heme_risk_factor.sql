CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_risk_factor
   OPTIONS(description='Contains risk factors for Hematology patient')
  AS SELECT
      cn_patient_heme_risk_factor.cn_patient_heme_diagnosis_sid,
      cn_patient_heme_risk_factor.nav_patient_id,
      cn_patient_heme_risk_factor.previous_tumor_site_id,
      cn_patient_heme_risk_factor.other_previous_tumor_site_id,
      cn_patient_heme_risk_factor.tumor_type_id,
      cn_patient_heme_risk_factor.diagnosis_result_id,
      cn_patient_heme_risk_factor.nav_diagnosis_id,
      cn_patient_heme_risk_factor.navigator_id,
      cn_patient_heme_risk_factor.coid,
      cn_patient_heme_risk_factor.company_code,
      cn_patient_heme_risk_factor.risk_factor_text,
      cn_patient_heme_risk_factor.other_risk_factor_text,
      cn_patient_heme_risk_factor.hashbite_ssk,
      cn_patient_heme_risk_factor.source_system_code,
      cn_patient_heme_risk_factor.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_risk_factor
  ;
