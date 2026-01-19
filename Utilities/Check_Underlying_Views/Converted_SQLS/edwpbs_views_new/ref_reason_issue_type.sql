-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_reason_issue_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_reason_issue_type AS SELECT
    a.reason_issue_type_id,
    a.reason_issue_type_code,
    a.reason_issue_type_desc,
    a.source_system_code,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_reason_issue_type AS a
;
