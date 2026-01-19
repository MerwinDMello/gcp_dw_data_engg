-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
