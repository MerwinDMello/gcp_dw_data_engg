CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_contact_method
   OPTIONS(description='Contains the distinct list of methods of contact that a navigator has.')
  AS SELECT
      ref_contact_method.contact_method_id,
      ref_contact_method.contact_method_desc,
      ref_contact_method.source_system_code,
      ref_contact_method.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_contact_method
  ;
