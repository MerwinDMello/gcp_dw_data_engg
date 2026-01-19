-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_adj_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_adj_dtl
   OPTIONS(description='Payment resolution adjustment details for all recoveries captured by Payment Resolution Application is maintained in the table.')
  AS SELECT
      payment_resolution_recovery_adj_dtl.payment_resolution_recovery_adj_id,
      payment_resolution_recovery_adj_dtl.reporting_date,
      payment_resolution_recovery_adj_dtl.rpt_freq_type_code,
      payment_resolution_recovery_adj_dtl.payment_resolution_recovery_id,
      payment_resolution_recovery_adj_dtl.company_code,
      payment_resolution_recovery_adj_dtl.coid,
      payment_resolution_recovery_adj_dtl.entry_type_code,
      payment_resolution_recovery_adj_dtl.reporting_period,
      payment_resolution_recovery_adj_dtl.valid_recovery_ind,
      ROUND(payment_resolution_recovery_adj_dtl.collection_amt, 3, 'ROUND_HALF_EVEN') AS collection_amt,
      ROUND(payment_resolution_recovery_adj_dtl.net_collection_amt, 3, 'ROUND_HALF_EVEN') AS net_collection_amt,
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
