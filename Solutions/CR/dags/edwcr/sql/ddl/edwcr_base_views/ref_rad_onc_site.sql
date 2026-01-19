CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_onc_site
   OPTIONS(description='Contains information for radiation oncology sites')
  AS SELECT
      ref_rad_onc_site.site_sk,
      ref_rad_onc_site.source_site_id,
      ref_rad_onc_site.source_site_guid_text,
      ref_rad_onc_site.site_code_text,
      ref_rad_onc_site.site_name,
      ref_rad_onc_site.server_name,
      ref_rad_onc_site.site_desc,
      ref_rad_onc_site.server_ip_address_text,
      ref_rad_onc_site.aura_version_text,
      ref_rad_onc_site.aura_last_installed_date_time,
      ref_rad_onc_site.registration_date_time,
      ref_rad_onc_site.history_user_name,
      ref_rad_onc_site.history_date_time,
      ref_rad_onc_site.source_system_code,
      ref_rad_onc_site.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_rad_onc_site
  ;
