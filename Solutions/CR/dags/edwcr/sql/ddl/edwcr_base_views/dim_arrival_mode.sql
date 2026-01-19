CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_arrival_mode AS SELECT
    dim_arrival_mode.arrival_mode_sk,
    dim_arrival_mode.arrival_mode_code,
    dim_arrival_mode.arrival_mode_desc,
    dim_arrival_mode.source_system_code,
    dim_arrival_mode.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_arrival_mode
;
