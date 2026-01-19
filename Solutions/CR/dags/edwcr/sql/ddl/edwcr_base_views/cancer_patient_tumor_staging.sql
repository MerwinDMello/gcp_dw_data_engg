CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_staging
   OPTIONS(description='Contains tumor stage details for patient navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID')
  AS SELECT
      cancer_patient_tumor_staging.cancer_patient_tumor_staging_sk,
      cancer_patient_tumor_staging.cancer_patient_tumor_driver_sk,
      cancer_patient_tumor_staging.cancer_patient_driver_sk,
      cancer_patient_tumor_staging.cancer_tumor_driver_sk,
      cancer_patient_tumor_staging.coid,
      cancer_patient_tumor_staging.company_code,
      cancer_patient_tumor_staging.best_cs_summary_desc,
      cancer_patient_tumor_staging.best_cs_tnm_desc,
      cancer_patient_tumor_staging.tumor_size_num_text,
      cancer_patient_tumor_staging.cancer_stage_code,
      cancer_patient_tumor_staging.cancer_stage_class_method_code,
      cancer_patient_tumor_staging.cancer_stage_type_code,
      cancer_patient_tumor_staging.cancer_stage_result_text,
      cancer_patient_tumor_staging.ajcc_stage_desc,
      cancer_patient_tumor_staging.tumor_size_summary_desc,
      cancer_patient_tumor_staging.source_system_code,
      cancer_patient_tumor_staging.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_staging
  ;
