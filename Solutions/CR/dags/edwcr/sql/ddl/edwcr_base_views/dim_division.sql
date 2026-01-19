CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_division AS SELECT
    dim_division.division_sk,
    dim_division.division_name,
    dim_division.group_sk,
    dim_division.valid_from_date_time,
    dim_division.external_ind,
    dim_division.active_ind,
    dim_division.source_system_code,
    dim_division.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_division
;
