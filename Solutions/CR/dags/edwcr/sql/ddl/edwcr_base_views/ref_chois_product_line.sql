CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_chois_product_line AS SELECT
    ref_chois_product_line.chois_product_line_code,
    ref_chois_product_line.chois_product_line_desc,
    ref_chois_product_line.source_system_code,
    ref_chois_product_line.dw_last_update_date_time
  FROM
    {{ params.param_pf_base_views_dataset_name }}.ref_chois_product_line
;
