-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/armap_recoveries.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.armap_recoveries AS SELECT
    armap_recoveries.rptg_period,
    armap_recoveries.unit_num,
    armap_recoveries.coid,
    armap_recoveries.agency_name,
    armap_recoveries.agency_type,
    armap_recoveries.recovery_amt
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.armap_recoveries
;
