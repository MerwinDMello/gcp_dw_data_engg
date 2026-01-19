CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_site_location
   OPTIONS(description='Contains a distinct list of treatment or biopsy sites.')
  AS SELECT
      ref_site_location.site_location_id,
      ref_site_location.site_location_desc,
      ref_site_location.source_system_code,
      ref_site_location.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_site_location
  ;
