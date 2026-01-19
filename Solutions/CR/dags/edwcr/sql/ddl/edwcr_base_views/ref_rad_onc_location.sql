CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_location
   OPTIONS(description='Contains information for radiation oncology location')
  AS SELECT
      ref_rad_onc_location.location_sk,
      ref_rad_onc_location.site_sk,
      ref_rad_onc_location.source_location_id,
      ref_rad_onc_location.country_name,
      ref_rad_onc_location.state_name,
      ref_rad_onc_location.city_name,
      ref_rad_onc_location.county_name,
      ref_rad_onc_location.zip_code,
      ref_rad_onc_location.log_id,
      ref_rad_onc_location.run_id,
      ref_rad_onc_location.source_system_code,
      ref_rad_onc_location.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_location
  ;
