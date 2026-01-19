-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/edw_user_request_transactions.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.edw_user_request_transactions AS SELECT
    edw_ad_user_group_info_ac.*
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.edw_ad_user_group_info_ac
;
