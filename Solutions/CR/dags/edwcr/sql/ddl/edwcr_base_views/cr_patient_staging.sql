CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_staging
   OPTIONS(description='This table contains tumor staging details of patient')
  AS SELECT
      cr_patient_staging.cr_patient_staging_sid,
      cr_patient_staging.cr_patient_id,
      cr_patient_staging.tumor_id,
      cr_patient_staging.ajcc_stage_id,
      cr_patient_staging.cancer_stage_classification_method_code,
      cr_patient_staging.cancer_stage_type_code,
      cr_patient_staging.cancer_stage_result_text,
      cr_patient_staging.source_system_code,
      cr_patient_staging.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_staging
  ;
