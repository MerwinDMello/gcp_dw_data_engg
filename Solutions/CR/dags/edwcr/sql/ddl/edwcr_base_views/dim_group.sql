CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_group AS SELECT
    dim_group.group_sk,
    dim_group.group_name,
    dim_group.valid_from_date_time,
    dim_group.external_ind,
    dim_group.active_ind,
    dim_group.source_system_code,
    dim_group.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_group
;
