-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_reason_code_xref.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_reason_code_xref AS SELECT
    a.reason_code_sid,
    a.source_system_code,
    a.source_reason_code,
    a.legacy_reason_id,
    a.reason_desc,
    a.reason_category_id,
    a.reason_issue_type_id,
    a.perm_discrepancy_flag,
    a.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_reason_code_xref AS a
;
