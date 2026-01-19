CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_lookup
   OPTIONS(description='Contains information for radiation oncology lookup data')
  AS SELECT
      ref_rad_onc_lookup.lookup_sk,
      ref_rad_onc_lookup.site_sk,
      ref_rad_onc_lookup.source_lookup_id,
      ref_rad_onc_lookup.table_name,
      ref_rad_onc_lookup.lookup_code_text,
      ref_rad_onc_lookup.lookup_desc,
      ref_rad_onc_lookup.log_id,
      ref_rad_onc_lookup.run_id,
      ref_rad_onc_lookup.source_system_code,
      ref_rad_onc_lookup.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_lookup
  ;
