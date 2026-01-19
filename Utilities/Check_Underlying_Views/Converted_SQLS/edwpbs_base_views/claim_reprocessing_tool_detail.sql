-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.claim_reprocessing_tool_detail
   OPTIONS(description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.')
  AS SELECT
      ROUND(claim_reprocessing_tool_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      claim_reprocessing_tool_detail.crt_log_id,
      claim_reprocessing_tool_detail.rpt_freq_type_code,
      claim_reprocessing_tool_detail.coid,
      claim_reprocessing_tool_detail.company_code,
      claim_reprocessing_tool_detail.unit_num,
      ROUND(claim_reprocessing_tool_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      claim_reprocessing_tool_detail.request_type_desc,
      claim_reprocessing_tool_detail.request_date_time,
      ROUND(claim_reprocessing_tool_detail.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      claim_reprocessing_tool_detail.last_activity_date_time,
      claim_reprocessing_tool_detail.status_desc,
      claim_reprocessing_tool_detail.discrepancy_date_time,
      claim_reprocessing_tool_detail.discrepancy_source_desc,
      claim_reprocessing_tool_detail.reimbursement_impact_desc,
      claim_reprocessing_tool_detail.reprocess_reason_text,
      claim_reprocessing_tool_detail.queue_name,
      claim_reprocessing_tool_detail.claim_category_type_code,
      claim_reprocessing_tool_detail.extract_date_time,
      claim_reprocessing_tool_detail.source_system_code,
      claim_reprocessing_tool_detail.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.claim_reprocessing_tool_detail
  ;
