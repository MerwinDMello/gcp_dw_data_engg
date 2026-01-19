CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_staging
   OPTIONS(description='Contains the details behind a patients level of cancer broken down into different stages.')
  AS SELECT
      cn_patient_staging.cn_patient_staging_sid,
      cn_patient_staging.cancer_stage_class_method_code,
      cn_patient_staging.cancer_staging_type_code,
      cn_patient_staging.diagnosis_result_id,
      cn_patient_staging.nav_patient_id,
      cn_patient_staging.tumor_type_id,
      cn_patient_staging.nav_diagnosis_id,
      cn_patient_staging.navigator_id,
      cn_patient_staging.coid,
      cn_patient_staging.company_code,
      cn_patient_staging.cancer_staging_result_code,
      cn_patient_staging.cancer_stage_code,
      cn_patient_staging.hashbite_ssk,
      cn_patient_staging.source_system_code,
      cn_patient_staging.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_staging
  ;
