-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_discrepancy_reason.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_discrepancy_reason AS SELECT
    ref_discrepancy_reason.discrepancy_reason_code,
    ref_discrepancy_reason.discrepancy_reason_desc,
    ref_discrepancy_reason.source_system_code,
    ref_discrepancy_reason.reason_category_code
  FROM
    `hca-hin-dev-cur-parallon`.edwpf_base_views.ref_discrepancy_reason
;
