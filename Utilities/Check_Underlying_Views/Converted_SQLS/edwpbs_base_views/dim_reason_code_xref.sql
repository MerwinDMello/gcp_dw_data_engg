-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/dim_reason_code_xref.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.dim_reason_code_xref
   OPTIONS(description='Crosswalk dimension table that captures discrepancy reasons from sources like Concuity, Medicare , Patient Accounting Discrepancy Reporting .')
  AS SELECT
      dim_reason_code_xref.reason_code_sid,
      dim_reason_code_xref.source_system_code,
      ROUND(dim_reason_code_xref.source_reason_code, 0, 'ROUND_HALF_EVEN') AS source_reason_code,
      ROUND(dim_reason_code_xref.legacy_reason_id, 0, 'ROUND_HALF_EVEN') AS legacy_reason_id,
      dim_reason_code_xref.reason_desc,
      dim_reason_code_xref.reason_category_id,
      dim_reason_code_xref.reason_issue_type_id,
      dim_reason_code_xref.perm_discrepancy_flag,
      dim_reason_code_xref.dw_last_update_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.dim_reason_code_xref
  ;
