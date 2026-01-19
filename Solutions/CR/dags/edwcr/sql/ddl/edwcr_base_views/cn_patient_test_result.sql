CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_test_result
   OPTIONS(description='Contains the test results for each patient.')
  AS SELECT
      cn_patient_test_result.nav_patient_test_result_sid,
      cn_patient_test_result.test_type_id,
      cn_patient_test_result.nav_patient_id,
      cn_patient_test_result.tumor_type_id,
      cn_patient_test_result.diagnosis_result_id,
      cn_patient_test_result.nav_diagnosis_id,
      cn_patient_test_result.navigator_id,
      cn_patient_test_result.coid,
      cn_patient_test_result.company_code,
      cn_patient_test_result.test_date,
      cn_patient_test_result.test_performed_ind,
      cn_patient_test_result.test_value_num,
      cn_patient_test_result.hashbite_ssk,
      cn_patient_test_result.source_system_code,
      cn_patient_test_result.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_test_result
  ;
