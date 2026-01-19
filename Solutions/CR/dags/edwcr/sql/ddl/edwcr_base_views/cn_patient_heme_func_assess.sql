CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_heme_func_assess
   OPTIONS(description='Contains all the functional assessment details for Hematology patient')
  AS SELECT
      cn_patient_heme_func_assess.cn_patient_heme_func_assess_sid,
      cn_patient_heme_func_assess.nav_patient_id,
      cn_patient_heme_func_assess.test_type_id,
      cn_patient_heme_func_assess.tumor_type_id,
      cn_patient_heme_func_assess.diagnosis_result_id,
      cn_patient_heme_func_assess.nav_diagnosis_id,
      cn_patient_heme_func_assess.navigator_id,
      cn_patient_heme_func_assess.coid,
      cn_patient_heme_func_assess.company_code,
      cn_patient_heme_func_assess.func_assess_date,
      cn_patient_heme_func_assess.test_type_result_amt,
      cn_patient_heme_func_assess.hashbite_ssk,
      cn_patient_heme_func_assess.source_system_code,
      cn_patient_heme_func_assess.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_heme_func_assess
  ;
