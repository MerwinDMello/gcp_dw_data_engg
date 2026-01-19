-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE OR REPLACE VIEW {{ params.param_pbs_views_dataset_name }}.claim_reprocessing_tool_detail
   OPTIONS(description='This table contains all detail related information derived from the claims reprocessing tool.  Data from this table is used to drive discrepancies in expected versus actual payment for a specific account.')
  AS SELECT
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.crt_log_id,
      a.rpt_freq_type_code,
      a.coid,
      a.company_code,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.request_type_desc,
      a.request_date_time,
      ROUND(a.financial_class_code, 0, 'ROUND_HALF_EVEN') AS financial_class_code,
      a.last_activity_date_time,
      a.status_desc,
      a.discrepancy_date_time,
      a.discrepancy_source_desc,
      a.reimbursement_impact_desc,
      a.reprocess_reason_text,
      a.queue_name,
      a.claim_category_type_code,
      a.extract_date_time,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      {{ params.param_pbs_base_views_dataset_name }}.claim_reprocessing_tool_detail AS a
      INNER JOIN {{ params.param_auth_base_views_dataset_name }}.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
  ;
