-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/payment_compliance_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.payment_compliance_credit_refund
   OPTIONS(description='This is a daily table that contains Credit Refunded patient accounts showing Refund amounts and other amounts.')
  AS SELECT
      a.credit_balance_refund_id AS credit_refund_id,
      ROUND(a.patient_dw_id, 0, 'ROUND_HALF_EVEN') AS patient_dw_id,
      a.reporting_date,
      a.company_code,
      a.coid,
      a.unit_num,
      ROUND(a.pat_acct_num, 0, 'ROUND_HALF_EVEN') AS pat_acct_num,
      a.admission_type_code,
      a.refund_type_sid,
      a.credit_status_sid,
      a.account_status_code,
      ROUND(a.payor_financial_class_code, 0, 'ROUND_HALF_EVEN') AS payor_financial_class_code,
      a.iplan_id_ins1,
      ROUND(a.refund_amt, 3, 'ROUND_HALF_EVEN') AS refund_amt,
      ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
      ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
      ROUND(a.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
      ROUND(a.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
      ROUND(a.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
      ROUND(a.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
      a.entered_date,
      a.discharge_date,
      a.admission_date,
      a.final_bill_date,
      a.resolved_date,
      a.bill_through_date,
      a.source_system_code,
      a.dw_last_update_date_time
    FROM
      `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_credit_inventory AS a
      INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
       AND upper(a.coid) = upper(b.co_id)
       AND b.user_id = session_user()
    WHERE upper(a.credit_balance_refund_ind) = 'R'
  ;
