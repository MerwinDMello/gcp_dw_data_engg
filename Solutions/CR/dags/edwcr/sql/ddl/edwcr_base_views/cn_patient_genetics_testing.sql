CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_genetics_testing
   OPTIONS(description='Contains the details behind the genetics testing a patient underwent.')
  AS SELECT
      cn_patient_genetics_testing.cn_patient_genetics_testing_sid,
      cn_patient_genetics_testing.breast_cancer_type_id,
      cn_patient_genetics_testing.core_record_type_id,
      cn_patient_genetics_testing.nav_patient_id,
      cn_patient_genetics_testing.tumor_type_id,
      cn_patient_genetics_testing.diagnosis_result_id,
      cn_patient_genetics_testing.nav_diagnosis_id,
      cn_patient_genetics_testing.navigator_id,
      cn_patient_genetics_testing.coid,
      cn_patient_genetics_testing.company_code,
      cn_patient_genetics_testing.testing_date,
      cn_patient_genetics_testing.test_name,
      cn_patient_genetics_testing.genetics_specialist_name,
      cn_patient_genetics_testing.comment_text,
      cn_patient_genetics_testing.hashbite_ssk,
      cn_patient_genetics_testing.source_system_code,
      cn_patient_genetics_testing.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_genetics_testing
  ;
