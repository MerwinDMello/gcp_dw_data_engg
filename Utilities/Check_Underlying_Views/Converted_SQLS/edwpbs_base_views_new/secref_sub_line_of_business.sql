-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/secref_sub_line_of_business.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.secref_sub_line_of_business AS SELECT
    secref_sub_line_of_business.company_code,
    secref_sub_line_of_business.user_id,
    secref_sub_line_of_business.sub_lob
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.secref_sub_line_of_business
;
