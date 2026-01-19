CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.cn_patient_contact
   OPTIONS(description='Contains all the communications to the patient or other points of contacts associated with the patient.')
  AS SELECT
      cn_patient_contact.cn_patient_contact_sid,
      cn_patient_contact.contact_purpose_id,
      cn_patient_contact.contact_method_id,
      cn_patient_contact.contact_person_id,
      cn_patient_contact.nav_patient_id,
      cn_patient_contact.tumor_type_id,
      cn_patient_contact.diagnosis_result_id,
      cn_patient_contact.nav_diagnosis_id,
      cn_patient_contact.navigator_id,
      cn_patient_contact.coid,
      cn_patient_contact.company_code,
      cn_patient_contact.contact_date,
      cn_patient_contact.other_purpose_detail_text,
      cn_patient_contact.other_person_contacted_text,
      cn_patient_contact.time_spent_amount_text,
      cn_patient_contact.comment_text,
      cn_patient_contact.hashbite_ssk,
      cn_patient_contact.source_system_code,
      cn_patient_contact.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.cn_patient_contact
  ;
