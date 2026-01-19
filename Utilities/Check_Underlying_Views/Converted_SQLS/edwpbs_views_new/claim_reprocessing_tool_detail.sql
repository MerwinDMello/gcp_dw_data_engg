-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/claim_reprocessing_tool_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.claim_reprocessing_tool_detail AS SELECT
    a.patient_dw_id,
    a.crt_log_id,
    a.rpt_freq_type_code,
    a.coid,
    a.company_code,
    a.unit_num,
    a.pat_acct_num,
    a.request_type_desc,
    a.request_date_time,
    a.financial_class_code,
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
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.claim_reprocessing_tool_detail AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
