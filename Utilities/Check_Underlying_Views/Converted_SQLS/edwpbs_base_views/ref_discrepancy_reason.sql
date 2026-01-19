-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
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
