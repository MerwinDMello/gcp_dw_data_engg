CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_major_diagnostic AS SELECT
    ref_major_diagnostic.drg_major_diag_cat_code,
    ref_major_diagnostic.major_diagnostic_category_desc,
    ref_major_diagnostic.source_system_code
  FROM
    {{ params.param_pf_base_views_dataset_name }}.ref_major_diagnostic
;
