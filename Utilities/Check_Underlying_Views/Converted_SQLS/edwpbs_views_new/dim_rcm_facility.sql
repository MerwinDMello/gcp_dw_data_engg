-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_rcm_facility.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_rcm_facility AS SELECT
    dim_rcm_facility.coid,
    dim_rcm_facility.market_code,
    dim_rcm_facility.coid_name,
    dim_rcm_facility.unit_num,
    dim_rcm_facility.company_code,
    dim_rcm_facility.dw_last_update_date_time,
    dim_rcm_facility.source_system_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_rcm_facility
;
