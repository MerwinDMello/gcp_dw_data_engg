CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_lookup_name
   OPTIONS(description='This table contains name of the column for which lookup is done')
  AS SELECT
      ref_lookup_name.lookup_sid,
      ref_lookup_name.lookup_name,
      ref_lookup_name.source_system_code,
      ref_lookup_name.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_lookup_name
  ;
