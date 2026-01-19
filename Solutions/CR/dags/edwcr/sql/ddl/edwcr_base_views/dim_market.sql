CREATE OR REPLACE VIEW {{ params.param_cr_base_views_dataset_name }}.dim_market AS SELECT
    dim_market.market_sk,
    dim_market.market_name,
    dim_market.division_sk,
    dim_market.external_ind,
    dim_market.active_ind,
    dim_market.valid_from_date_time,
    dim_market.source_system_code,
    dim_market.dw_last_update_date_time
  FROM
    {{ params.param_auth_base_views_dataset_name }}.dim_market
;
