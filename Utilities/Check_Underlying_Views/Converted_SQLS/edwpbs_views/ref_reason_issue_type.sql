-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/ref_reason_issue_type.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.ref_reason_issue_type
   OPTIONS(description='Reference table to maitain the type of the Issue causing the Discrepancy')
  AS SELECT
      a.reason_issue_type_id,
      a.reason_issue_type_code,
      a.reason_issue_type_desc,
      a.source_system_code,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.ref_reason_issue_type AS a
  ;
