CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_address
   OPTIONS(description='Contains the address of the patients navigated by Sarah Cannon through Metriq')
  AS SELECT
      cr_patient_address.cr_patient_id,
      cr_patient_address.state_id,
      cr_patient_address.address_line_1_text,
      cr_patient_address.address_line_2_text,
      cr_patient_address.city_name,
      cr_patient_address.zip_code,
      cr_patient_address.source_system_code,
      cr_patient_address.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_address
  ;
