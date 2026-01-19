CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_address
   OPTIONS(description='Contains the address of the patients navigated by Sarah Cannon')
  AS SELECT
      cn_patient_address.nav_patient_id,
      cn_patient_address.housing_type_id,
      cn_patient_address.address_line_1_text,
      cn_patient_address.address_line_2_text,
      cn_patient_address.city_name,
      cn_patient_address.state_code,
      cn_patient_address.zip_code,
      cn_patient_address.local_housing_address_text,
      cn_patient_address.source_system_code,
      cn_patient_address.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_address
  ;
