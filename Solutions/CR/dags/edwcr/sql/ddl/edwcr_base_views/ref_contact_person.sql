CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_contact_person
   OPTIONS(description='Contains the distinct list of people a navigator contacted associated with a patient communication.')
  AS SELECT
      ref_contact_person.contact_person_id,
      ref_contact_person.contact_person_desc,
      ref_contact_person.source_system_code,
      ref_contact_person.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_contact_person
  ;
