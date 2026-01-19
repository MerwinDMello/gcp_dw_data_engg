-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_base_views/payment_compliance_credit_refund.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_base_views.payment_compliance_credit_refund AS SELECT
    payment_compliance_credit_refund.credit_refund_id,
    payment_compliance_credit_refund.patient_dw_id,
    payment_compliance_credit_refund.reporting_date,
    payment_compliance_credit_refund.company_code,
    payment_compliance_credit_refund.coid,
    payment_compliance_credit_refund.unit_num,
    payment_compliance_credit_refund.pat_acct_num,
    payment_compliance_credit_refund.admission_type_code,
    payment_compliance_credit_refund.refund_type_sid,
    payment_compliance_credit_refund.credit_status_sid,
    payment_compliance_credit_refund.account_status_code,
    payment_compliance_credit_refund.payor_financial_class_code,
    payment_compliance_credit_refund.iplan_id_ins1,
    payment_compliance_credit_refund.refund_amt,
    payment_compliance_credit_refund.total_charge_amt,
    payment_compliance_credit_refund.total_account_balance_amt,
    payment_compliance_credit_refund.total_cash_pay_amt,
    payment_compliance_credit_refund.total_allow_amt,
    payment_compliance_credit_refund.total_policy_adj_amt,
    payment_compliance_credit_refund.total_write_off_amt,
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
