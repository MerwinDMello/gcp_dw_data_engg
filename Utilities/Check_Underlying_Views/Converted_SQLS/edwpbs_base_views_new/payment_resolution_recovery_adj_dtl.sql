-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_adj_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_adj_dtl AS SELECT
    payment_resolution_recovery_adj_dtl.payment_resolution_recovery_adj_id,
    payment_resolution_recovery_adj_dtl.reporting_date,
    payment_resolution_recovery_adj_dtl.rpt_freq_type_code,
    payment_resolution_recovery_adj_dtl.payment_resolution_recovery_id,
    payment_resolution_recovery_adj_dtl.company_code,
    payment_resolution_recovery_adj_dtl.coid,
    payment_resolution_recovery_adj_dtl.entry_type_code,
    payment_resolution_recovery_adj_dtl.reporting_period,
    payment_resolution_recovery_adj_dtl.valid_recovery_ind,
    payment_resolution_recovery_adj_dtl.collection_amt,
    payment_resolution_recovery_adj_dtl.net_collection_amt,
    payment_resolution_recovery_adj_dtl.executive_approval_status_ind,
    payment_resolution_recovery_adj_dtl.created_by_user_id,
    payment_resolution_recovery_adj_dtl.created_date_time,
    payment_resolution_recovery_adj_dtl.updated_by_user_id,
    payment_resolution_recovery_adj_dtl.updated_by_date_time,
    payment_resolution_recovery_adj_dtl.recovery_comment_text,
    payment_resolution_recovery_adj_dtl.source_system_code,
    payment_resolution_recovery_adj_dtl.dw_last_update_date_time
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs.payment_resolution_recovery_adj_dtl
;
