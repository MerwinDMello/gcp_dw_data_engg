-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/ref_reason_issue_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_reason_issue_type
   OPTIONS(description='Reference table to maitain the type of the Issue causing the Discrepancy')
  AS SELECT
      ref_reason_issue_type.reason_issue_type_id,
      ref_reason_issue_type.reason_issue_type_code,
      ref_reason_issue_type.reason_issue_type_desc,
      ref_reason_issue_type.source_system_code,
      ref_reason_issue_type.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.ref_reason_issue_type
  ;
