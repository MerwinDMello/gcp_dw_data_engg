-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/dim_reason_code_xref.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.dim_reason_code_xref
   OPTIONS(description='Crosswalk dimension table that captures discrepancy reasons from sources like Concuity, Medicare , Patient Accounting Discrepancy Reporting .')
  AS SELECT
      a.reason_code_sid,
      a.source_system_code,
      ROUND(a.source_reason_code, 0, 'ROUND_HALF_EVEN') AS source_reason_code,
      ROUND(a.legacy_reason_id, 0, 'ROUND_HALF_EVEN') AS legacy_reason_id,
      a.reason_desc,
      a.reason_category_id,
      a.reason_issue_type_id,
      a.perm_discrepancy_flag,
      a.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_reason_code_xref AS a
  ;
