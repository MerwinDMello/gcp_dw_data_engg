CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_admit_source AS SELECT
    dim_admit_source.admit_source_sk,
    dim_admit_source.admit_source_code,
    dim_admit_source.admit_source_desc,
    dim_admit_source.source_system_code,
    dim_admit_source.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_admit_source
;
