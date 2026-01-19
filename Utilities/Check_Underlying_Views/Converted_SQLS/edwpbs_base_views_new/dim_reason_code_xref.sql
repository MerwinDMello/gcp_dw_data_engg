-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_reason_code_xref.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_reason_code_xref AS SELECT
    dim_reason_code_xref.reason_code_sid,
    dim_reason_code_xref.source_system_code,
    dim_reason_code_xref.source_reason_code,
    dim_reason_code_xref.legacy_reason_id,
    dim_reason_code_xref.reason_desc,
    dim_reason_code_xref.reason_category_id,
    dim_reason_code_xref.reason_issue_type_id,
    dim_reason_code_xref.perm_discrepancy_flag,
    dim_reason_code_xref.dw_last_update_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.dim_reason_code_xref
;
