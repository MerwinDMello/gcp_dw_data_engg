CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_religion AS SELECT
    dim_religion.religion_sk,
    dim_religion.religion_code,
    dim_religion.religion_desc,
    dim_religion.source_system_code,
    dim_religion.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_religion
;
