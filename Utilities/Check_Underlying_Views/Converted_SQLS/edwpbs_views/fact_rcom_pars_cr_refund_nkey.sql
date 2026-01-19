-- Translation time: 2023-09-22T18:46:04.447603Z
-- Translation job ID: ba460d1f-d301-46b6-9c47-810d266c2894
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_cr_refund_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_cr_refund_nkey AS SELECT
    ROUND(a.patient_sid, 0, 'ROUND_HALF_EVEN') AS patient_sid,
    a.date_sid,
    a.pe_date,
    a.unit_num_sid,
    a.unit_num_member,
    a.unit_num_alias,
    a.account_status_sid,
    a.account_status_member,
    a.account_status_alias,
    a.patient_type_sid,
    a.patient_type_member,
    a.patient_type_alias,
    a.payor_sid,
    a.payor_member,
    a.payor_alias,
    a.payor_sequence_sid,
    a.payor_sequence_member,
    a.payor_sequence_alias,
    a.same_store_sid,
    a.same_store_name_child,
    a.same_store_alias_name,
    a.payor_financial_class_sid,
    a.payor_financial_class_member,
    a.payor_financial_class_alias,
    a.refund_type_sid,
    a.refund_type_member,
    a.refund_type_alias,
    a.cr_refund_age_month_sid,
    a.cr_refund_age_member,
    a.cr_refund_age_alias,
    a.company_code,
    a.coid,
    a.credit_refund_id,
    a.age_of_refund,
    ROUND(a.refund_amount, 3, 'ROUND_HALF_EVEN') AS refund_amount,
    a.refund_count,
    ROUND(a.total_account_balance_amt, 3, 'ROUND_HALF_EVEN') AS total_account_balance_amt,
    ROUND(a.total_charge_amt, 3, 'ROUND_HALF_EVEN') AS total_charge_amt,
    ROUND(a.total_cash_pay_amt, 3, 'ROUND_HALF_EVEN') AS total_cash_pay_amt,
    ROUND(a.total_allow_amt, 3, 'ROUND_HALF_EVEN') AS total_allow_amt,
    ROUND(a.total_policy_adj_amt, 3, 'ROUND_HALF_EVEN') AS total_policy_adj_amt,
    ROUND(a.total_write_off_amt, 3, 'ROUND_HALF_EVEN') AS total_write_off_amt,
    a.discharge_date,
    a.credit_balance_date,
    a.resolved_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_cr_refund_nkey AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON upper(a.company_code) = upper(b.company_code)
     AND upper(a.coid) = upper(b.co_id)
     AND b.user_id = session_user()
;
