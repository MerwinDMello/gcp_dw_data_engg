-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/edw_user_request_transactions.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.edw_user_request_transactions AS SELECT
    edw_ad_user_group_info_ac.*
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.edw_ad_user_group_info_ac
;
