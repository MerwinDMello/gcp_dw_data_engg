CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_tumor_driver
   OPTIONS(description='Contains consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID and tumor information for them')
  AS SELECT
      cancer_patient_tumor_driver.cancer_patient_tumor_driver_sk,
      cancer_patient_tumor_driver.cancer_patient_driver_sk,
      cancer_patient_tumor_driver.cancer_tumor_driver_sk,
      cancer_patient_tumor_driver.coid,
      cancer_patient_tumor_driver.company_code,
      cancer_patient_tumor_driver.cr_patient_id,
      cancer_patient_tumor_driver.cn_patient_id,
      cancer_patient_tumor_driver.cp_patient_id,
      cancer_patient_tumor_driver.cr_tumor_primary_site_id,
      cancer_patient_tumor_driver.cn_tumor_type_id,
      cancer_patient_tumor_driver.cp_icd_oncology_code,
      cancer_patient_tumor_driver.source_system_code,
      cancer_patient_tumor_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_tumor_driver
  ;
