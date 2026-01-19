-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_collection_recall_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_collection_recall_reason
   OPTIONS(description='Table contains related  Recall reason detail for all the accounts  recalled by the agency')
  AS SELECT
      a.recall_reason_code,
      a.recall_reason_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_collection_recall_reason AS a
  ;
