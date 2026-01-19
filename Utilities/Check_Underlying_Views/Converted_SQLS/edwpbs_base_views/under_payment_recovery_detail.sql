-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/under_payment_recovery_detail.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.under_payment_recovery_detail
   OPTIONS(description='Any recovery that is captured by SSC at the patient account level is maintained in the table.')
  AS SELECT
      under_payment_recovery_detail.month_id,
      ROUND(under_payment_recovery_detail.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      under_payment_recovery_detail.iplan_id,
      under_payment_recovery_detail.under_payment_recovery_date,
      under_payment_recovery_detail.company_code,
      under_payment_recovery_detail.coid,
      under_payment_recovery_detail.unit_num,
      ROUND(under_payment_recovery_detail.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      ROUND(under_payment_recovery_detail.under_payment_recovery_amt, 3, 'ROUND_HALF_EVEN') AS under_payment_recovery_amt,
      under_payment_recovery_detail.source_system_code,
      under_payment_recovery_detail.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.under_payment_recovery_detail
  ;
