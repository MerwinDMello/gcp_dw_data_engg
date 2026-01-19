CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_detail_tumor_site
   OPTIONS(description='Reference for the detail location of the tumor site.')
  AS SELECT
      ref_detail_tumor_site.detail_tumor_site_id,
      ref_detail_tumor_site.detail_tumor_site_desc,
      ref_detail_tumor_site.source_system_code,
      ref_detail_tumor_site.dw_last_update_date_time
    FROM
      {{ params.param_cr_core_dataset_name }}.ref_detail_tumor_site
  ;
