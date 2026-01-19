CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.rad_onc_patient_contact
   OPTIONS(description='Contains the patient contact person details')
  AS SELECT
      rad_onc_patient_contact.patient_contact_sk,
      rad_onc_patient_contact.patient_sk,
      rad_onc_patient_contact.contact_address_sk,
      rad_onc_patient_contact.contact_full_name,
      rad_onc_patient_contact.contact_relation_text,
      rad_onc_patient_contact.contact_entrusted_ind,
      rad_onc_patient_contact.contact_comment_text,
      rad_onc_patient_contact.source_system_code,
      rad_onc_patient_contact.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.rad_onc_patient_contact
  ;
