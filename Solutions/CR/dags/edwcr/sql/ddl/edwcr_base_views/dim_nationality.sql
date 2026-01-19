CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_nationality AS SELECT
    dim_nationality.nationality_sk,
    dim_nationality.nationality_code,
    dim_nationality.nationality_desc,
    dim_nationality.source_system_code,
    dim_nationality.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_nationality
;
