CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_tumor_site
   OPTIONS(description='Site of the tumor.')
  AS SELECT
      ref_tumor_site.tumor_site_id,
      ref_tumor_site.tumor_site_desc,
      ref_tumor_site.source_system_code,
      ref_tumor_site.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_tumor_site
  ;
