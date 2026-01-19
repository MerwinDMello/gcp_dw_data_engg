CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_junc_patient_contact_phone
   OPTIONS(description='This table will be a junction for patient contact phone numbers')
  AS SELECT
      rad_onc_junc_patient_contact_phone.patient_contact_sk,
      rad_onc_junc_patient_contact_phone.phone_num_type_code,
      rad_onc_junc_patient_contact_phone.phone_num_sk,
      rad_onc_junc_patient_contact_phone.source_system_code,
      rad_onc_junc_patient_contact_phone.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_junc_patient_contact_phone
  ;
