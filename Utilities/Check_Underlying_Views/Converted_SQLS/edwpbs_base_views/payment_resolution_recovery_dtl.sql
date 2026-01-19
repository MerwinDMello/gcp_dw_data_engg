-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_resolution_recovery_dtl.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_resolution_recovery_dtl
   OPTIONS(description='Payment resolution amounts captured by Payment Resolution Application at the transaction level is maintained in the table.')
  AS SELECT
      payment_resolution_recovery_dtl.payment_resolution_recovery_id,
      payment_resolution_recovery_dtl.reporting_date,
      payment_resolution_recovery_dtl.rpt_freq_type_code,
      ROUND(payment_resolution_recovery_dtl.payor_dw_id, 0, 'ROUND_HALF_EVEN') AS payor_dw_id,
      ROUND(payment_resolution_recovery_dtl.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_resolution_recovery_dtl.company_code,
      payment_resolution_recovery_dtl.coid,
      payment_resolution_recovery_dtl.unit_num,
      payment_resolution_recovery_dtl.created_by_user_id,
      payment_resolution_recovery_dtl.created_date_time,
      payment_resolution_recovery_dtl.file_id,
      payment_resolution_recovery_dtl.invalid_ind,
      payment_resolution_recovery_dtl.month_id,
      payment_resolution_recovery_dtl.iplan_id,
      ROUND(payment_resolution_recovery_dtl.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_resolution_recovery_dtl.financial_class_sid,
      payment_resolution_recovery_dtl.patient_type_sid,
      payment_resolution_recovery_dtl.payment_date,
      payment_resolution_recovery_dtl.payor_sid,
      ROUND(payment_resolution_recovery_dtl.reason_code_sid, 0, 'ROUND_HALF_EVEN') AS reason_code_sid,
      payment_resolution_recovery_dtl.updated_by_user_id,
      payment_resolution_recovery_dtl.updated_by_date_time,
      payment_resolution_recovery_dtl.source_system_code,
      payment_resolution_recovery_dtl.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.payment_resolution_recovery_dtl
  ;
