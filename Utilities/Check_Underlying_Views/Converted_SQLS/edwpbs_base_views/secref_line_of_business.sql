-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_line_of_business.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_line_of_business AS SELECT
    secref_line_of_business.company_code,
    secref_line_of_business.user_id,
    secref_line_of_business.lob
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_line_of_business
;
