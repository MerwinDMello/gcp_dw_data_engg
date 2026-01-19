-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_reason_issue_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_reason_issue_type AS SELECT
    ref_reason_issue_type.reason_issue_type_id,
    ref_reason_issue_type.reason_issue_type_code,
    ref_reason_issue_type.reason_issue_type_desc,
    ref_reason_issue_type.source_system_code,
    ref_reason_issue_type.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.ref_reason_issue_type
;
