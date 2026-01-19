CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cr_patient_contact
   OPTIONS(description='Contains the patient contact person details')
  AS SELECT
      cr_patient_contact.patient_contact_id,
      cr_patient_contact.cr_patient_id,
      cr_patient_contact.contact_relation_id,
      cr_patient_contact.contact_type_id,
      cr_patient_contact.contact_num_code,
      cr_patient_contact.contact_first_name,
      cr_patient_contact.contact_last_name,
      cr_patient_contact.contact_middle_name,
      cr_patient_contact.preferred_contact_method_text,
      cr_patient_contact.source_system_code,
      cr_patient_contact.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cr_patient_contact
  ;
