-- Translation time: 2024-01-12T17:51:56.897679Z
-- Translation job ID: daf02731-0647-4415-a27a-5b3d10f518dd
-- Source: internal_metastore/db_hca-hin-dev-cur-parallon/schema_edwpbs_views/fact_rcom_pars_cr_refund_nkey.memory
-- Translated from: Teradata
-- Translated to: BigQuery

CREATE VIEW IF NOT EXISTS `hca-hin-dev-cur-parallon`.edwpbs_views.fact_rcom_pars_cr_refund_nkey AS SELECT
    a.patient_sid,
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
    a.refund_amount,
    a.refund_count,
    a.total_account_balance_amt,
    a.total_charge_amt,
    a.total_cash_pay_amt,
    a.total_allow_amt,
    a.total_policy_adj_amt,
    a.total_write_off_amt,
    a.discharge_date,
    a.credit_balance_date,
    a.resolved_date
  FROM
    `hca-hin-dev-cur-parallon`.edwpbs_base_views.fact_rcom_pars_cr_refund_nkey AS a
    INNER JOIN `hca-hin-dev-cur-parallon`.edwpf_base_views.secref_facility AS b ON a.company_code = b.company_code
     AND a.coid = b.co_id
     AND b.user_id = session_user()
;
