-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/lu_rcm_market.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.lu_rcm_market AS SELECT
    lu_rcm_market.market_code,
    lu_rcm_market.division_code,
    lu_rcm_market.market_name,
    lu_rcm_market.d_level,
    lu_rcm_market.dw_last_update_date_time,
    lu_rcm_market.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.lu_rcm_market
;
