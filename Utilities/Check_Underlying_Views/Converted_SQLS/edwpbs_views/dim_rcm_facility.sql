-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
