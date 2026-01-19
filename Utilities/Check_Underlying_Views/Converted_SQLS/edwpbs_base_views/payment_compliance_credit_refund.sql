-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_credit_refund
   OPTIONS(description='This is a daily table that contains Credit Refunded patient accounts showing Refund amounts and other amounts.')
  AS SELECT
      payment_compliance_credit_refund.credit_refund_id,
      ROUND(payment_compliance_credit_refund.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      payment_compliance_credit_refund.reporting_date,
      payment_compliance_credit_refund.company_code,
      payment_compliance_credit_refund.coid,
      payment_compliance_credit_refund.unit_num,
      ROUND(payment_compliance_credit_refund.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      payment_compliance_credit_refund.admission_type_code,
      payment_compliance_credit_refund.refund_type_sid,
      payment_compliance_credit_refund.credit_status_sid,
      payment_compliance_credit_refund.account_status_code,
      ROUND(payment_compliance_credit_refund.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      payment_compliance_credit_refund.iplan_id_ins1,
      ROUND(payment_compliance_credit_refund.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      ROUND(payment_compliance_credit_refund.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(payment_compliance_credit_refund.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(payment_compliance_credit_refund.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(payment_compliance_credit_refund.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(payment_compliance_credit_refund.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(payment_compliance_credit_refund.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      payment_compliance_credit_refund.entered_date,
      payment_compliance_credit_refund.discharge_date,
      payment_compliance_credit_refund.admission_date,
      payment_compliance_credit_refund.final_bill_date,
      payment_compliance_credit_refund.credit_balance_date,
      payment_compliance_credit_refund.resolved_date,
      payment_compliance_credit_refund.bill_through_date,
      payment_compliance_credit_refund.source_system_code,
      payment_compliance_credit_refund.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs.payment_compliance_credit_refund
  ;
