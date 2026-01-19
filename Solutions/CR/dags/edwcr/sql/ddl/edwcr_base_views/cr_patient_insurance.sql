CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_insurance
   OPTIONS(description='This table contains insurance details used by patient')
  AS SELECT
      cr_patient_insurance.cr_patient_id,
      cr_patient_insurance.tumor_id,
      cr_patient_insurance.insurance_type_id,
      cr_patient_insurance.insurance_company_id,
      cr_patient_insurance.source_system_code,
      cr_patient_insurance.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_insurance
  ;
