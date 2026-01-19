-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_resolution_recovery_adj_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_resolution_recovery_adj_dtl AS SELECT
    a.payment_resolution_recovery_adj_id,
    a.reporting_date,
    a.rpt_freq_type_code,
    a.payment_resolution_recovery_id,
    a.company_code,
    a.coid,
    a.entry_type_code,
    a.reporting_period,
    a.valid_recovery_ind,
    a.collection_amt,
    a.net_collection_amt,
    a.executive_approval_status_ind,
    a.created_by_user_id,
    a.created_date_time,
    a.updated_by_user_id,
    a.updated_by_date_time,
    a.recovery_comment_text,
    a.source_system_code,
    a.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_adj_dtl AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
