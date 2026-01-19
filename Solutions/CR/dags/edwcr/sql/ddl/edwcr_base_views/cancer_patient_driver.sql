CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cancer_patient_driver
   OPTIONS(description='Contains consolidated patients navigated by Sarah Cannon through Metriq iNavigate and Cancer Patient ID.This will be the driver table for reporting.')
  AS SELECT
      cancer_patient_driver.cancer_patient_driver_sk,
      cancer_patient_driver.cr_patient_id,
      cancer_patient_driver.cp_patient_id,
      cancer_patient_driver.cn_patient_id,
      cancer_patient_driver.coid,
      cancer_patient_driver.company_code,
      cancer_patient_driver.network_mnemonic_cs,
      cancer_patient_driver.patient_market_urn_text,
      cancer_patient_driver.medical_record_num,
      cancer_patient_driver.empi_text,
      cancer_patient_driver.patient_first_name,
      cancer_patient_driver.patient_middle_name,
      cancer_patient_driver.patient_last_name,
      cancer_patient_driver.preferred_name,
      cancer_patient_driver.source_system_code,
      cancer_patient_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cancer_patient_driver
  ;
