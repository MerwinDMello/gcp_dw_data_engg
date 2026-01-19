CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_date_driver
   OPTIONS(description='This table is a bridge between Sarah Cannon Patient Identifiers and Clinical Data Management (CDM) Patient Identifiers.')
  AS SELECT
      cr_patient_date_driver.cancer_patient_driver_sk,
      cr_patient_date_driver.patient_dw_id,
      cr_patient_date_driver.cancer_diagnosis_date,
      cr_patient_date_driver.cancer_diagnosis_90_day_prior_date,
      cr_patient_date_driver.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_date_driver
  ;
