CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_primary_language AS SELECT
    dim_primary_language.primary_language_sk,
    dim_primary_language.primary_language_code,
    dim_primary_language.primary_language_desc,
    dim_primary_language.source_system_code,
    dim_primary_language.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_primary_language
;
