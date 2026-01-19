CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_conditioning_type AS SELECT
    dim_conditioning_type.conditioning_type_sk,
    dim_conditioning_type.conditioning_type_code,
    dim_conditioning_type.conditioning_type_desc,
    dim_conditioning_type.source_system_code,
    dim_conditioning_type.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_conditioning_type
;
