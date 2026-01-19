-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_sector.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_sector AS SELECT
    secref_sector.company_code,
    secref_sector.user_id,
    secref_sector.sector_number
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_sector
;
