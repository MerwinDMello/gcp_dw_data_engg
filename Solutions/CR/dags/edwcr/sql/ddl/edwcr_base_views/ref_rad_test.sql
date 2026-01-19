CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.ref_rad_test AS SELECT
    ref_rad_test.rad_cpt_test_code,
    ref_rad_test.rad_test_desc,
    ref_rad_test.rad_test_prop_desc,
    ref_rad_test.source_system_code,
    ref_rad_test.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.ref_rad_test
;
