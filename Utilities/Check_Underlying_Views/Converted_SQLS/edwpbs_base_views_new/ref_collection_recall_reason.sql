-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_collection_recall_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_collection_recall_reason AS SELECT
    ref_collection_recall_reason.recall_reason_code,
    ref_collection_recall_reason.recall_reason_desc,
    ref_collection_recall_reason.source_system_code,
    ref_collection_recall_reason.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_collection_recall_reason
;
