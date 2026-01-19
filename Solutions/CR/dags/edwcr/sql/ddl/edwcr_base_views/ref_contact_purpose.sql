CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_contact_purpose
   OPTIONS(description='Contains the distinct list of purposes associated with a patient communication.')
  AS SELECT
      ref_contact_purpose.contact_purpose_id,
      ref_contact_purpose.contact_purpose_desc,
      ref_contact_purpose.source_system_code,
      ref_contact_purpose.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_contact_purpose
  ;
