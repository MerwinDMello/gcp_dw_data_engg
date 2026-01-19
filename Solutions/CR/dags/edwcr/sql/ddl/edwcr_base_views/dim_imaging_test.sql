CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_imaging_test AS SELECT
    dim_imaging_test.imaging_test_sk,
    dim_imaging_test.imaging_test_code,
    dim_imaging_test.imaging_test_desc,
    dim_imaging_test.source_system_code,
    dim_imaging_test.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_imaging_test
;
