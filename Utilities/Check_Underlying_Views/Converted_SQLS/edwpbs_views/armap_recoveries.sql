-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/armap_recoveries.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.armap_recoveries AS SELECT
    armap_recoveries.rptg_period,
    armap_recoveries.unit_num,
    armap_recoveries.coid,
    armap_recoveries.agency_type,
    ROUND(armap_recoveries.recovery_amt, 3, 'ROUND_HALF_EVEN') AS agency_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries
;
