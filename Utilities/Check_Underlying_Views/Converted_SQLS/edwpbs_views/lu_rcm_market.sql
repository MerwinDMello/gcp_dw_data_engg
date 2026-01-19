-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/lu_rcm_market.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.lu_rcm_market AS SELECT
    lu_rcm_market.market_code,
    lu_rcm_market.division_code,
    lu_rcm_market.market_name,
    lu_rcm_market.d_level,
    lu_rcm_market.dw_last_update_date_time,
    lu_rcm_market.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_market
;
