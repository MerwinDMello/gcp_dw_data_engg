-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_cm_physician_peer_review_status.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_cm_physician_peer_review_status
   OPTIONS(description='Table contains related Physician\'s Peer Review Status descriptions')
  AS SELECT
      a.peer_review_status_code,
      a.peer_review_status_desc,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_cm_physician_peer_review_status AS a
  ;
